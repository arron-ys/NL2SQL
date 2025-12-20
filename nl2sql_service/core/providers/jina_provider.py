"""
Jina Provider Module

实现 Jina AI 的提供商适配器（主要用于嵌入）。
"""
import asyncio
import os
import socket
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Deque, Dict, List, Optional
from urllib.parse import urlparse

import anyio
import httpx

from core.errors import AppError
from utils.log_manager import get_logger
from .base import BaseAIProvider

logger = get_logger(__name__)


# ============================================================
# 统计指标数据类
# ============================================================
@dataclass
class ProviderMetrics:
    """Provider 级别统计指标（内存计数 + 滑窗统计）"""
    # 总量计数
    requests_total: int = 0
    success_total: int = 0
    failure_total: int = 0
    retry_total: int = 0
    consecutive_failures: int = 0
    
    # 最近错误信息
    last_error_type: Optional[str] = None
    last_error_ts: Optional[float] = None
    last_error_message: Optional[str] = None
    
    # 滑窗统计（最近 WINDOW_SEC 秒内的错误时间戳）
    recent_errors: Deque[float] = field(default_factory=deque)
    window_sec: int = 300  # 默认 5 分钟滑窗
    
    # 健康检查
    healthcheck_total: int = 0
    healthcheck_fail: int = 0
    last_healthcheck_ts: Optional[float] = None
    last_healthcheck_ok: bool = True
    
    def record_request_start(self):
        """记录请求开始"""
        self.requests_total += 1
    
    def record_success(self):
        """记录成功"""
        self.success_total += 1
        self.consecutive_failures = 0
    
    def record_failure(self, error_type: str, error_message: str):
        """记录失败"""
        now = time.time()
        self.failure_total += 1
        self.consecutive_failures += 1
        self.last_error_type = error_type
        self.last_error_ts = now
        self.last_error_message = error_message[:200]  # 限制长度
        
        # 加入滑窗
        self.recent_errors.append(now)
        self._cleanup_window()
    
    def record_retry(self):
        """记录重试"""
        self.retry_total += 1
    
    def record_healthcheck(self, success: bool):
        """记录健康检查"""
        self.healthcheck_total += 1
        if not success:
            self.healthcheck_fail += 1
        self.last_healthcheck_ts = time.time()
        self.last_healthcheck_ok = success
    
    def _cleanup_window(self):
        """清理过期的滑窗数据"""
        now = time.time()
        cutoff = now - self.window_sec
        while self.recent_errors and self.recent_errors[0] < cutoff:
            self.recent_errors.popleft()
    
    def get_error_rate(self) -> float:
        """获取滑窗内错误率"""
        self._cleanup_window()
        if self.requests_total == 0:
            return 0.0
        # 简化：用滑窗内错误数除以总请求数（近似）
        # 更精确的做法需要记录请求时间戳，这里简化处理
        recent_error_count = len(self.recent_errors)
        if recent_error_count == 0:
            return 0.0
        # 粗略估算：假设请求均匀分布在 window 内
        return min(recent_error_count / max(self.requests_total, 1), 1.0)
    
    def check_alert_threshold(self, error_rate_threshold: float = 0.10, consecutive_threshold: int = 3) -> bool:
        """检查是否需要告警"""
        error_rate = self.get_error_rate()
        return (error_rate >= error_rate_threshold) or (self.consecutive_failures >= consecutive_threshold)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于日志）"""
        return {
            "requests_total": self.requests_total,
            "success_total": self.success_total,
            "failure_total": self.failure_total,
            "retry_total": self.retry_total,
            "consecutive_failures": self.consecutive_failures,
            "error_rate": round(self.get_error_rate(), 4),
            "last_error_type": self.last_error_type,
            "last_error_ts": self.last_error_ts,
            "last_error_message": self.last_error_message,
            "healthcheck_total": self.healthcheck_total,
            "healthcheck_fail": self.healthcheck_fail,
            "last_healthcheck_ts": self.last_healthcheck_ts,
            "last_healthcheck_ok": self.last_healthcheck_ok,
        }


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
        
        # 初始化统计指标
        self.metrics = ProviderMetrics()
        
        # 设置超时时间（优先级：LLM_TIMEOUT > JINA_TIMEOUT > 默认值 30.0）
        timeout_str = os.getenv("LLM_TIMEOUT") or os.getenv("JINA_TIMEOUT", "30.0")
        self.timeout = float(timeout_str)

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
                logger.debug(
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
                logger.debug(
                    "Jina proxy unreachable; downgraded to direct connection with trust_env=False",
                    extra={"provider": "jina"},
                )

        # ============================================================
        # 配置连接池和重试机制（Option A：连接层治本）
        # ============================================================
        # 配置连接池限制（keepalive_expiry 保持默认 5s，不调大）
        limits = httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=5.0,  # 保持默认短超时，失败由 transport retries 吸收
        )
        
        # 配置重试传输层（处理陈旧连接、TLS 握手失败等）
        transport = httpx.AsyncHTTPTransport(
            retries=2,  # 连接失败时重试 2 次
            limits=limits,
        )
        
        # 初始化异步 HTTP 客户端（显式设置 trust_env）
        client_kwargs: Dict[str, Any] = {
            "timeout": self.timeout,
            "trust_env": bool(self._trust_env),
            "limits": limits,
            "transport": transport,
        }
        if self._proxy_source == "explicit" and self._proxy_url:
            client_kwargs["proxy"] = self._proxy_url
        
        self._client = httpx.AsyncClient(**client_kwargs)
        self._client_lock = asyncio.Lock()  # 用于保护 reset_client()
        
        logger.debug(
            "JinaProvider initialized",
            extra={
                "base_url": self.base_url,
                "timeout": self.timeout,
                "retries": 2,
                "keepalive_expiry": 5.0,
            }
        )
        
        # 代理详细信息记录到 DEBUG 级别
        logger.debug(
            "JinaProvider proxy configuration",
            extra={
                "proxy_mode": self._proxy_mode,
                "proxy_strict": self._proxy_strict,
                "trust_env": self._trust_env,
                "proxy_source": self._proxy_source,
                "proxy_url": self._proxy_url,
                "proxy_downgraded": self._proxy_downgraded,
                "proxy_disabled_reason": self._proxy_disabled_reason,
            }
        )
    
    async def reset_client(self):
        """
        重置 HTTP 客户端（用于恢复失效连接）
        
        在检测到连接层异常后调用，强制清理并重新创建客户端。
        """
        async with self._client_lock:
            try:
                await self._client.aclose()
                logger.debug("Jina HTTP client closed for reset")
            except Exception as e:
                logger.warning(f"Error closing Jina client during reset: {e}")
            
            # 重新创建客户端（复用初始化逻辑）
            limits = httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=5.0,
            )
            
            transport = httpx.AsyncHTTPTransport(
                retries=2,
                limits=limits,
            )
            
            client_kwargs: Dict[str, Any] = {
                "timeout": self.timeout,
                "trust_env": bool(self._trust_env),
                "limits": limits,
                "transport": transport,
            }
            if self._proxy_source == "explicit" and self._proxy_url:
                client_kwargs["proxy"] = self._proxy_url
            
            self._client = httpx.AsyncClient(**client_kwargs)
            logger.info("Jina HTTP client reset successfully")
    
    async def embed(
        self,
        texts: List[str],
        model: str = "jina-embeddings-v3",
        **kwargs: Any
    ) -> List[List[float]]:
        """
        生成文本嵌入向量（带统计和兜底重试）
        
        Option A'：对连接层异常做一次轻量兜底重试，覆盖：
        - anyio.EndOfStream
        - httpx.ConnectError
        - httpx.ConnectTimeout
        - httpx.ReadTimeout
        
        HTTPStatusError（4xx/5xx）不重试，避免把业务错误当网络抖动。
        """
        # 检测 NO_NETWORK 环境变量（用于非 live 测试）
        no_network = os.getenv("NO_NETWORK", "").lower() in ("1", "true", "yes")
        if no_network:
            raise RuntimeError(
                f"Network call detected in offline test mode (NO_NETWORK=1). "
                f"Provider: jina, Method: embed, Model: {model}. "
                f"Please mock the provider method before calling."
            )
        if not self.api_key:
            raise ValueError("JINA_API_KEY is required")
        
        start_time = time.time()
        self.metrics.record_request_start()
        retry_attempt = 0
        last_error = None
        
        for attempt in range(2):  # 最多 2 次尝试（1 次正常 + 1 次兜底重试）
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
                
                # 成功：记录统计
                elapsed_ms = (time.time() - start_time) * 1000
                self.metrics.record_success()
                
                logger.debug(
                    "Jina embedding generated",
                    extra={
                        "model": model,
                        "text_count": len(texts),
                        "dimension": len(embeddings[0]) if embeddings else 0,
                        "elapsed_ms": round(elapsed_ms, 2),
                        "retry_attempt": retry_attempt,
                        "metrics": self.metrics.to_dict(),
                    }
                )
                
                return embeddings
            
            except httpx.HTTPStatusError as e:
                # HTTP 错误（4xx/5xx）：不重试，直接抛出
                error_type = type(e).__name__
                error_msg = str(e)
                self.metrics.record_failure(error_type, error_msg)
                
                logger.error(
                    "Jina embedding HTTP error (no retry for HTTP status errors)",
                    extra={
                        "error": error_msg,
                        "error_type": error_type,
                        "status_code": e.response.status_code,
                        "response": e.response.text[:200] if e.response else None,
                        "metrics": self.metrics.to_dict(),
                    }
                )
                
                # 检查是否需要告警
                if self.metrics.check_alert_threshold():
                    logger.error(
                        "ALERT: Jina provider error rate threshold exceeded",
                        extra={"metrics_snapshot": self.metrics.to_dict()}
                    )
                
                raise JinaEmbeddingError(
                    "Jina embedding HTTP error",
                    details={
                        "provider": "jina",
                        "base_url": self.base_url,
                        "api_url": self.api_url,
                        "status_code": e.response.status_code if e.response else None,
                        "response_preview": e.response.text[:200] if e.response else None,
                        "error": error_msg,
                        "metrics": self.metrics.to_dict(),
                    },
                ) from e
            
            except (anyio.EndOfStream, httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                # 连接层异常：记录并考虑重试
                error_type = type(e).__name__
                error_msg = str(e)
                last_error = e
                
                if attempt == 0:
                    # 第一次失败：记录重试，reset_client，继续循环
                    retry_attempt = 1
                    self.metrics.record_retry()
                    
                    logger.warning(
                        f"Jina embedding connection error (attempt {attempt + 1}/2), will reset client and retry",
                        extra={
                            "error": error_msg,
                            "error_type": error_type,
                            "model": model,
                        }
                    )
                    
                    # 重置客户端（清理失效连接）
                    await self.reset_client()
                    continue  # 重试
                else:
                    # 第二次还失败：记录失败，抛出异常
                    self.metrics.record_failure(error_type, error_msg)
                    
                    logger.error(
                        f"Jina embedding connection error after retry (attempt {attempt + 1}/2)",
                        extra={
                            "error": error_msg,
                            "error_type": error_type,
                            "model": model,
                            "metrics": self.metrics.to_dict(),
                        }
                    )
                    
                    # 检查是否需要告警
                    if self.metrics.check_alert_threshold():
                        logger.error(
                            "ALERT: Jina provider error rate threshold exceeded",
                            extra={"metrics_snapshot": self.metrics.to_dict()}
                        )
                    
                    diag = {
                        "provider": "jina",
                        "base_url": self.base_url,
                        "api_url": self.api_url,
                        "proxy_mode": self._proxy_mode,
                        "trust_env": self._trust_env,
                        "proxy_source": self._proxy_source,
                        "proxy_url": self._proxy_url,
                        "error_type": error_type,
                        "error": error_msg,
                        "model": model,
                        "retry_attempt": retry_attempt,
                        "metrics": self.metrics.to_dict(),
                    }
                    msg = (
                        "Jina embedding failed after connection error retry "
                        f"(provider=jina, base_url={self.base_url}, proxy_mode={self._proxy_mode}, "
                        f"trust_env={self._trust_env}, proxy_source={self._proxy_source}, proxy_url={self._proxy_url}). "
                        f"Underlying error: {error_type}: {error_msg}"
                    )
                    raise JinaEmbeddingError(msg, details=diag) from e
            
            except Exception as e:
                # 其他未预期异常：记录失败，直接抛出（不重试）
                error_type = type(e).__name__
                error_msg = str(e)
                self.metrics.record_failure(error_type, error_msg)
                
                logger.error(
                    "Jina embedding unexpected error",
                    extra={
                        "error": error_msg,
                        "error_type": error_type,
                        "model": model,
                        "metrics": self.metrics.to_dict(),
                    }
                )
                
                # 检查是否需要告警
                if self.metrics.check_alert_threshold():
                    logger.error(
                        "ALERT: Jina provider error rate threshold exceeded",
                        extra={"metrics_snapshot": self.metrics.to_dict()}
                    )
                
                diag = {
                    "provider": "jina",
                    "base_url": self.base_url,
                    "api_url": self.api_url,
                    "error_type": error_type,
                    "error": error_msg,
                    "model": model,
                    "metrics": self.metrics.to_dict(),
                }
                msg = (
                    "Jina embedding failed "
                    f"(provider=jina, base_url={self.base_url}). "
                    f"Underlying error: {error_type}: {error_msg}"
                )
                raise JinaEmbeddingError(msg, details=diag) from e
        
        # 不应该到达这里
        raise JinaEmbeddingError(
            "Jina embedding failed after all retries",
            details={"last_error": str(last_error) if last_error else "unknown"}
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
        # 检测 NO_NETWORK 环境变量（用于非 live 测试）
        no_network = os.getenv("NO_NETWORK", "").lower() in ("1", "true", "yes")
        if no_network:
            raise RuntimeError(
                f"Network call detected in offline test mode (NO_NETWORK=1). "
                f"Provider: jina, Method: chat, Model: {model}. "
                f"Please mock the provider method before calling."
            )
        raise NotImplementedError("JinaProvider does not support chat yet.")
    
    async def chat_json(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        # 检测 NO_NETWORK 环境变量（用于非 live 测试）
        no_network = os.getenv("NO_NETWORK", "").lower() in ("1", "true", "yes")
        if no_network:
            raise RuntimeError(
                f"Network call detected in offline test mode (NO_NETWORK=1). "
                f"Provider: jina, Method: chat_json, Model: {model}. "
                f"Please mock the provider method before calling."
            )
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
        """关闭 HTTP 客户端（资源管理）"""
        async with self._client_lock:
            try:
                await self._client.aclose()
                logger.debug("Jina HTTP client closed")
            except Exception as e:
                logger.warning(f"Error closing Jina client: {e}")
    
    async def healthcheck(self) -> bool:
        """
        健康检查（长期：连接健康检查 + 自愈）
        
        对 Jina API 做零成本/低成本的连通性探测：
        - 发起 HEAD 请求到 embeddings endpoint（不关心状态码，只关心能否完成 TLS）
        - 若探测抛出连接层异常：记录 healthcheck_fail，调用 reset_client()
        
        Returns:
            bool: True 表示健康，False 表示不健康
        """
        try:
            # 发起 HEAD 请求（不关心状态码，只要能连上就行）
            response = await self._client.head(
                self.api_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=5.0,  # 短超时
            )
            # 只要没抛异常就认为健康（即使返回 4xx/5xx）
            self.metrics.record_healthcheck(success=True)
            logger.debug(
                "jina 连接正常",
                extra={
                    "status_code": response.status_code,
                    "metrics": self.metrics.to_dict(),
                }
            )
            return True
        
        except (anyio.EndOfStream, httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            # 连接层异常：记录失败，触发 reset_client
            error_type = type(e).__name__
            error_msg = str(e)
            self.metrics.record_healthcheck(success=False)
            
            logger.debug(
                "jina 连接异常，正在重置客户端",
                extra={
                    "error": error_msg,
                    "error_type": error_type,
                    "metrics": self.metrics.to_dict(),
                }
            )
            
            # 主动清理失效连接
            await self.reset_client()
            return False
        
        except Exception as e:
            # 其他异常：记录但不触发 reset（可能是临时问题）
            error_type = type(e).__name__
            error_msg = str(e)
            self.metrics.record_healthcheck(success=False)
            
            logger.debug(
                "jina 连接异常（未预期错误）",
                extra={
                    "error": error_msg,
                    "error_type": error_type,
                    "metrics": self.metrics.to_dict(),
                }
            )
            return False

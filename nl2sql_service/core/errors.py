"""
Project-level error definitions.

This module defines AppError, the unified internal exception type that can be
converted into a structured HTTP error response by FastAPI exception handlers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


_DETAILS_ALLOWLIST = {
    # network/provider diagnostics
    "provider",
    "base_url",
    "api_url",
    "proxy_mode",
    "proxy_strict",
    "trust_env",
    "proxy_source",
    "proxy_url",
    "proxy_downgraded",
    "proxy_disabled_reason",
    "explicit_proxy_configured",
    "system_env_proxy_present",
    # generic
    "error_summary",
    "error_type",
    "retryable",
    "status_code",
}


def sanitize_details(details: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Sanitize details for HTTP responses:
    - Only return allowlisted keys
    - Never return secrets/tokens/headers/bodies (enforced by allowlist)
    """
    if not details or not isinstance(details, dict):
        return None
    safe: Dict[str, Any] = {}
    for k, v in details.items():
        if k in _DETAILS_ALLOWLIST:
            safe[k] = v
    return safe or None


@dataclass
class AppError(Exception):
    """
    Project-level base exception.

    Fields:
    - code: stable error code for callers
    - message: human readable message (safe to return)
    - error_stage: stable stage id aligned with pipeline stages (e.g., STAGE_2_PLAN_GENERATION)
    - details: optional sanitized details for diagnostics (must be allowlist-safe)
    - status_code: HTTP status code (default 500). Keep existing semantics.
    """

    code: str
    message: str
    error_stage: str
    details: Optional[Dict[str, Any]] = None
    status_code: int = 500

    def __str__(self) -> str:  # for logs only
        return self.message


# ============================================================
# Provider-level stable exceptions (for fallback logic)
# ============================================================

class ProviderConnectionError(Exception):
    """
    稳定的 Provider 连接异常（用于 fallback 逻辑）
    
    替代 OpenAI SDK 的 APIConnectionError，避免因 SDK 版本变化导致测试脆弱。
    """
    def __init__(self, message: str, provider: str = None, original_error: Exception = None):
        super().__init__(message)
        self.provider = provider
        self.original_error = original_error


class ProviderRateLimitError(Exception):
    """
    稳定的 Provider 限流异常（用于 fallback 逻辑）
    
    替代 OpenAI SDK 的 RateLimitError，避免因 SDK 版本变化导致测试脆弱。
    """
    def __init__(self, message: str, provider: str = None, original_error: Exception = None):
        super().__init__(message)
        self.provider = provider
        self.original_error = original_error


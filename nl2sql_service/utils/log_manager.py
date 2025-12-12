"""
Logging Management Module

基于 loguru 实现结构化日志系统，支持异步上下文追踪。
使用 contextvars 存储 request_id，确保在异步环境中正确传递上下文。
"""
import contextvars
import sys
from contextlib import contextmanager
from typing import Optional

from loguru import logger

# ============================================================
# ContextVar 定义
# ============================================================
# 使用 ContextVar 存储请求 ID，支持异步上下文传递
request_id_var = contextvars.ContextVar("request_id", default="system")

# ============================================================
# 日志格式化字符串
# ============================================================
# 日志格式：从 extra 字典中读取 request_id
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "[{extra[request_id]}] | "
    "<cyan>{name}:{function}:{line}</cyan> - "
    "<level>{message}</level>"
)

# ============================================================
# 核心配置函数
# ============================================================
def configure_logger():
    """
    配置 loguru logger
    
    核心逻辑：
    1. 移除默认处理器
    2. 使用 filter 函数在 handler 级别注入 request_id
    3. filter 函数从 contextvars 中获取 request_id 并注入到 record["extra"] 中
    4. 添加新的处理器，输出到 stdout
    """
    # 移除默认的 handler
    logger.remove()
    
    # 定义 filter 函数：在 handler 级别注入 request_id
    def inject_request_id(record):
        """在 handler 级别注入 request_id 到日志记录的 extra 字典中"""
        # 确保 extra 字典存在
        if "extra" not in record:
            record["extra"] = {}
        # 从 contextvars 获取 request_id 并注入
        # 如果 extra 中已有 request_id，则不覆盖（允许显式设置）
        if "request_id" not in record["extra"]:
            record["extra"]["request_id"] = request_id_var.get()
        return True  # 返回 True 表示不过滤这条日志
    
    # 添加自定义格式的 handler，输出到 stdout
    # 使用 filter 参数在 handler 级别注入 request_id
    logger.add(
        sys.stdout,
        format=LOG_FORMAT,
        filter=inject_request_id,  # 使用 filter 在 handler 级别注入
        level="INFO",
        colorize=True,
        enqueue=True,  # 支持多进程/多线程安全
    )


# ============================================================
# Helper 函数
# ============================================================
def get_logger(name: Optional[str] = None):
    """
    获取日志记录器实例
    
    Args:
        name: 可选的模块名称（当前实现中未使用，保留用于未来扩展）
    
    Returns:
        Logger: loguru logger 实例
    """
    return logger


def set_request_id(request_id: str) -> None:
    """
    设置当前上下文的请求 ID
    
    Args:
        request_id: 请求唯一标识符
    """
    request_id_var.set(request_id)


def get_request_id() -> str:
    """
    获取当前上下文的请求 ID
    
    Returns:
        str: 当前请求 ID，如果没有设置则返回 "system"
    """
    return request_id_var.get()


# ============================================================
# 上下文管理器
# ============================================================
@contextmanager
def LogContext(request_id: str):
    """
    日志上下文管理器
    
    用于在 FastAPI 中间件或其他场景中设置请求 ID 上下文。
    进入上下文时设置 request_id，退出时恢复为默认值。
    
    Usage:
        ```python
        with LogContext("req-123"):
            logger.info("This log will have request_id=req-123")
        logger.info("This log will have request_id=system")
        ```
    
    Args:
        request_id: 请求唯一标识符
    """
    # 设置新的 request_id，返回 token 用于后续恢复
    token = request_id_var.set(request_id)
    try:
        yield
    finally:
        # 恢复旧的上下文值
        try:
            request_id_var.reset(token)
        except (ValueError, LookupError):
            # 如果 reset 失败（例如 token 无效），设置为默认值
            request_id_var.set("system")


# ============================================================
# 自动执行配置
# ============================================================
# 在模块导入时自动配置日志系统
configure_logger()

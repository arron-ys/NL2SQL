"""
Logging Management Module

基于 loguru 实现结构化日志系统，支持异步上下文追踪。
使用 contextvars 存储 request_id，确保在异步环境中正确传递上下文。
"""
import contextvars
import os
import sys
from contextlib import contextmanager
from typing import Optional

from loguru import logger

# ============================================================
# 设置控制台编码为 UTF-8（修复中文乱码问题）
# ============================================================
if sys.platform == "win32":
    # Windows 系统需要设置控制台编码为 UTF-8
    try:
        # 设置标准输出和标准错误输出为 UTF-8
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
        # 设置环境变量
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    except Exception:
        # 如果设置失败，忽略错误（某些环境可能不支持）
        pass

# ============================================================
# PID Guard：防止多进程/热重载场景下重复配置
# ============================================================
# 记录已配置的进程 ID，确保每个进程只配置一次
_CONFIGURED_PID: Optional[int] = None

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
    1. 使用 PID guard 确保每个进程只配置一次
    2. 从环境变量读取日志级别（LOG_LEVEL 或 LOGURU_LEVEL），默认 INFO
    3. 验证日志级别是否在白名单内，非法值回退到 INFO
    4. 移除默认处理器
    5. 使用 filter 函数在 handler 级别注入 request_id
    6. filter 函数从 contextvars 中获取 request_id 并注入到 record["extra"] 中
    7. 添加新的处理器，输出到 stdout
    
    注意：使用 PID guard 而非简单的布尔标志，以支持：
    - 热重载场景（uvicorn --reload）
    - 多 worker 场景（gunicorn --workers N）
    - gunicorn --preload 场景（主进程和 worker 进程都需要配置）
    """
    global _CONFIGURED_PID
    
    # PID Guard：检查当前进程是否已配置
    current_pid = os.getpid()
    if _CONFIGURED_PID == current_pid:
        # 当前进程已配置，直接返回
        return
    
    # 读取环境变量决定日志级别
    # 优先读取 LOG_LEVEL，其次 LOGURU_LEVEL，默认 INFO
    log_level = os.getenv("LOG_LEVEL") or os.getenv("LOGURU_LEVEL") or "INFO"
    log_level = log_level.upper()
    
    # 允许的日志级别白名单
    valid_levels = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
    
    # 验证日志级别，非法值回退到 INFO
    if log_level not in valid_levels:
        log_level = "INFO"
    
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
    # 注意：loguru 的 add() 方法不支持 encoding 参数，编码由 sys.stdout 的编码决定
    # 我们已经在模块开头设置了 sys.stdout 的编码为 UTF-8
    logger.add(
        sys.stdout,
        format=LOG_FORMAT,
        filter=inject_request_id,  # 使用 filter 在 handler 级别注入
        level=log_level,  # 使用从环境变量读取的日志级别
        colorize=True,
        enqueue=True,  # 支持多进程/多线程安全
    )
    
    # 打印配置信息（此时 request_id 应为默认值 "system"）
    logger.info("Logger configured: level={}", log_level)
    
    # 标记当前进程已配置
    _CONFIGURED_PID = current_pid


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

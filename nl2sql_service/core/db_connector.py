"""
Database Connector Module

管理 SQLAlchemy 异步连接池，提供统一的数据库连接接口。
支持 MySQL 和 PostgreSQL 两种数据库方言。
"""
import os
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from config.pipeline_config import get_pipeline_config, SupportedDialects
from utils.log_manager import get_logger

logger = get_logger(__name__)

# 全局引擎实例（单例）
_engine: Optional[AsyncEngine] = None


def _build_connection_string(db_type: SupportedDialects) -> str:
    """
    构建数据库连接字符串
    
    Args:
        db_type: 数据库类型（mysql 或 postgresql）
    
    Returns:
        str: SQLAlchemy 异步连接字符串
    
    Raises:
        ValueError: 当必需的配置项缺失时
    """
    # 从环境变量读取配置
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "3306"))
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "")
    
    if not db_name:
        raise ValueError("DB_NAME is required. Set it in .env file.")
    
    # 根据数据库类型构建连接字符串
    if db_type == SupportedDialects.MYSQL:
        # MySQL 使用 aiomysql 驱动
        # 格式: mysql+aiomysql://user:password@host:port/database
        connection_string = (
            f"mysql+aiomysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
    elif db_type == SupportedDialects.POSTGRESQL:
        # PostgreSQL 使用 asyncpg 驱动
        # 格式: postgresql+asyncpg://user:password@host:port/database
        connection_string = (
            f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    return connection_string


def get_engine(tenant_id: Optional[str] = None) -> AsyncEngine:
    """
    获取数据库引擎实例（单例模式）
    
    Args:
        tenant_id: 租户 ID，当前版本未使用，保留用于未来多租户支持
    
    Returns:
        AsyncEngine: SQLAlchemy 异步引擎实例
    
    Raises:
        ValueError: 当配置错误时
        RuntimeError: 当引擎初始化失败时
    """
    global _engine
    
    if _engine is None:
        try:
            # 获取配置
            config = get_pipeline_config()
            db_type = config.db_type
            
            # 构建连接字符串
            connection_string = _build_connection_string(db_type)
            
            # 创建异步引擎
            # pool_pre_ping=True: 连接池在每次使用前检查连接是否有效
            # pool_size: 连接池大小，默认 5
            # max_overflow: 允许超出 pool_size 的连接数，默认 10
            _engine = create_async_engine(
                connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                echo=False,  # 设置为 True 可以打印 SQL 语句（用于调试）
            )
            
            logger.info(
                "Database engine initialized",
                extra={
                    "db_type": db_type.value,
                    "tenant_id": tenant_id
                }
            )
        
        except Exception as e:
            logger.error(
                "Failed to initialize database engine",
                extra={"error": str(e)}
            )
            raise RuntimeError(f"Database engine initialization failed: {e}") from e
    
    return _engine


async def close_all() -> None:
    """
    关闭所有数据库连接
    
    在应用关闭时调用，释放连接池资源。
    """
    global _engine
    
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        logger.info("Database engine disposed")


# ============================================================
# 便捷函数：获取数据库会话
# ============================================================
@asynccontextmanager
async def get_db_session():
    """
    获取数据库会话（用于依赖注入）
    
    Yields:
        AsyncSession: SQLAlchemy 异步会话
    
    Note:
        这是一个异步上下文管理器，用于 FastAPI 的依赖注入。
        使用示例：
        ```python
        async with get_db_session() as session:
            result = await session.execute(text("SELECT 1"))
        ```
        
        在 FastAPI 路由中使用：
        ```python
        @app.get("/items")
        async def get_items(session: AsyncSession = Depends(get_db_session)):
            ...
        ```
    """
    from sqlalchemy.ext.asyncio import AsyncSession
    
    engine = get_engine()
    async with AsyncSession(engine) as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise



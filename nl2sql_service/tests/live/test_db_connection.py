"""
Database Connection Test Suite

测试数据库连接是否正常，包括：
- 连接字符串构建
- 数据库引擎初始化
- 实际连接测试（执行简单查询）

需要真实的数据库配置（从 .env 文件读取），如果配置不可用则跳过测试。
"""
import os
from pathlib import Path

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine

from core.db_connector import get_engine, get_db_session
import core.db_connector as db_module
from config.pipeline_config import get_pipeline_config, SupportedDialects


# ============================================================
# Skip Conditions (方案1：健壮的环境变量加载)
# ============================================================

def _should_skip_db_tests():
    """
    检查是否应该跳过数据库测试
    
    如果数据库配置缺失或为占位符值，则跳过测试。
    
    注意：这个函数在 pytest 收集阶段执行（@pytest.mark.skipif），此时 fixture 还没运行，
    所以需要手动加载 .env 文件。
    """
    # 获取项目根目录（nl2sql_service 目录）- 使用绝对路径
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / ".env"
    
    # 【方案1改进】只要 .env 文件存在，就加载（不覆盖已有环境变量）
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=False)
    
    # 检查必需配置是否存在
    db_host = os.getenv("DB_HOST", "")
    db_port = os.getenv("DB_PORT", "")
    db_user = os.getenv("DB_USER", "")
    db_name = os.getenv("DB_NAME", "")
    
    if not all([db_host, db_port, db_user, db_name]):
        return True
    
    # 检查 DB_NAME 是否为占位符值
    placeholder_db_names = ["", "your_database", "test", "example"]
    if db_name.lower() in placeholder_db_names:
        return True
    
    return False


# ============================================================
# Fixture (方案2：单例重置版)
# ============================================================

@pytest_asyncio.fixture(scope="function")
async def db_engine():
    """
    提供一个干净的数据库引擎。
    
    测试结束后自动关闭，并重置全局单例，防止污染下一个测试。
    这样每个测试都拥有属于自己的、绑定到当前 Event Loop 的引擎。
    """
    # 1. 获取引擎（可能是新建的，也可能是脏的单例）
    engine = get_engine()
    
    # 2. 提供给测试使用
    yield engine
    
    # 3. 清理阶段：关闭引擎
    await engine.dispose()
    
    # 4. 【关键】重置全局变量，强制下一次 get_engine() 重新创建
    db_module._engine = None


# ============================================================
# Test Cases
# ============================================================


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.live
@pytest.mark.skipif(
    _should_skip_db_tests(),
    reason="Database configuration not available or is placeholder"
)
async def test_database_connection(db_engine: AsyncEngine):
    """
    测试数据库连接是否正常
    
    验证：
    1. 能够成功初始化数据库引擎
    2. 能够建立实际连接
    3. 能够执行简单查询
    """
    config = get_pipeline_config()
    db_type = config.db_type
    
    # 从环境变量读取连接信息（用于日志输出）
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "3306")
    db_user = os.getenv("DB_USER", "root")
    db_name = os.getenv("DB_NAME", "")
    
    print(f"\n测试数据库连接:")
    print(f"  类型: {db_type.value}")
    print(f"  主机: {db_host}:{db_port}")
    print(f"  用户: {db_user}")
    print(f"  数据库: {db_name}")
    
    # 测试：建立连接并执行简单查询
    async with AsyncSession(db_engine) as session:
        # 执行一个简单的查询（根据数据库类型选择）
        if db_type == SupportedDialects.MYSQL:
            result = await session.execute(text("SELECT 1 as test_value"))
        elif db_type == SupportedDialects.POSTGRESQL:
            result = await session.execute(text("SELECT 1 as test_value"))
        else:
            pytest.fail(f"不支持的数据库类型: {db_type}")
        
        row = result.fetchone()
        assert row is not None, "查询未返回结果"
        assert row[0] == 1, f"查询结果不正确: {row[0]}"
        print("  ✓ 数据库连接测试成功")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.live
@pytest.mark.skipif(
    _should_skip_db_tests(),
    reason="Database configuration not available or is placeholder"
)
async def test_database_connection_with_get_db_session(db_engine: AsyncEngine):
    """
    测试使用 get_db_session() 辅助函数进行数据库连接
    
    验证 get_db_session() 函数能够正常工作。
    
    注意：虽然这个测试主要验证 get_db_session()，但我们仍然需要 db_engine fixture
    来确保测试后的清理和单例重置。
    """
    config = get_pipeline_config()
    db_type = config.db_type
    db_name = os.getenv("DB_NAME", "")
    
    print(f"\n测试 get_db_session() 函数:")
    print(f"  数据库类型: {db_type.value}")
    print(f"  数据库名称: {db_name}")
    
    # 使用 get_db_session() 获取会话（这是一个异步上下文管理器）
    # 注意：get_db_session() 内部会调用 get_engine()，由于单例模式，
    # 它会返回 db_engine fixture 创建的同一个引擎
    async with get_db_session() as session:
        # 执行简单查询
        if db_type == SupportedDialects.MYSQL:
            result = await session.execute(text("SELECT DATABASE() as current_db"))
        elif db_type == SupportedDialects.POSTGRESQL:
            result = await session.execute(text("SELECT current_database() as current_db"))
        else:
            pytest.fail(f"不支持的数据库类型: {db_type}")
        
        row = result.fetchone()
        assert row is not None, "查询未返回结果"
        current_db = row[0]
        assert current_db == db_name, f"当前数据库不匹配: 期望 {db_name}, 实际 {current_db}"
        print(f"  ✓ get_db_session() 测试成功，当前数据库: {current_db}")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.live
@pytest.mark.skipif(
    _should_skip_db_tests(),
    reason="Database configuration not available or is placeholder"
)
async def test_database_version(db_engine: AsyncEngine):
    """
    测试获取数据库版本信息
    
    验证能够成功查询数据库版本，确认连接正常且数据库可访问。
    """
    config = get_pipeline_config()
    db_type = config.db_type
    
    print(f"\n测试数据库版本查询:")
    print(f"  数据库类型: {db_type.value}")
    
    async with AsyncSession(db_engine) as session:
        # 根据数据库类型查询版本
        if db_type == SupportedDialects.MYSQL:
            result = await session.execute(text("SELECT VERSION() as version"))
        elif db_type == SupportedDialects.POSTGRESQL:
            result = await session.execute(text("SELECT version() as version"))
        else:
            pytest.fail(f"不支持的数据库类型: {db_type}")
        
        row = result.fetchone()
        assert row is not None, "查询未返回结果"
        version = row[0]
        assert version is not None and len(version) > 0, "版本信息为空"
        print(f"  ✓ 数据库版本: {version}")

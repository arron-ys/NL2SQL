"""
【简述】
验证数据库连接正常性：连接字符串构建、引擎初始化、实际连接测试与版本查询（需要真实数据库配置）。

【范围/不测什么】
- 不是 mock 测试；必须配置真实数据库连接，否则跳过。

【用例概述】
- test_database_connection:
  -- 验证能够成功初始化引擎、建立连接并执行简单查询
- test_database_connection_with_get_db_session:
  -- 验证通过 get_db_session 创建会话并查询数据
- test_database_version:
  -- 验证能够查询数据库版本信息
"""

import os

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine

from core.db_connector import get_engine, get_db_session
import core.db_connector as db_module
from config.pipeline_config import get_pipeline_config, SupportedDialects


# ============================================================
# Skip Conditions
# ============================================================

def _should_skip_db_tests():
    """
    检查是否应该跳过数据库测试
    
    如果数据库配置缺失或为占位符值，则跳过测试。
    
    注意：.env 文件由 tests/conftest.py 统一管理，在运行 live 测试时会自动加载。
    此函数在 pytest 收集阶段执行（@pytest.mark.skipif），此时 .env 应该已经被加载。
    """
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
@pytest.mark.live
@pytest.mark.skipif(
    _should_skip_db_tests(),
    reason="Database configuration not available or is placeholder"
)
async def test_database_connection(db_engine: AsyncEngine):
    """
    【测试目标】
    1. 验证能够成功初始化数据库引擎、建立连接并执行简单查询

    【执行过程】
    1. 使用 db_engine fixture 获取引擎
    2. 通过 engine.connect() 建立连接
    3. 执行 SELECT 1 测试查询
    4. 验证查询结果

    【预期结果】
    1. 连接成功建立
    2. SELECT 1 返回结果为 [(1,)]
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
@pytest.mark.live
@pytest.mark.skipif(
    _should_skip_db_tests(),
    reason="Database configuration not available or is placeholder"
)
async def test_database_connection_with_get_db_session(db_engine: AsyncEngine):
    """
    【测试目标】
    1. 验证通过 get_db_session 创建会话并查询数据

    【执行过程】
    1. 使用 get_db_session() 异步上下文管理器获取会话
    2. 根据数据库类型执行 SELECT DATABASE() 或 current_database() 查询
    3. 验证返回的数据库名称与配置一致
    4. db_engine fixture 确保测试后清理和单例重置

    【预期结果】
    1. 会话创建成功
    2. 查询返回当前数据库名称
    3. 数据库名称与配置的 DB_NAME 一致
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
@pytest.mark.live
@pytest.mark.skipif(
    _should_skip_db_tests(),
    reason="Database configuration not available or is placeholder"
)
async def test_database_version(db_engine: AsyncEngine):
    """
    【测试目标】
    1. 验证能够查询数据库版本信息

    【执行过程】
    1. 使用 db_engine 创建会话
    2. 根据数据库类型执行 SELECT VERSION() 或 version() 查询
    3. 验证返回的版本字符串不为空

    【预期结果】
    1. 版本查询成功
    2. 版本字符串不为空且长度 > 0
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

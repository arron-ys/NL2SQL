"""
【简述】
验证MySQL和PostgreSQL方言差异在SQL生成中的正确处理，包括时间函数、引用符、日期格式等。

【范围/不测什么】
- 不覆盖真实数据库连接；仅验证SQL字符串生成符合各方言规范。

【用例概述】
- test_mysql_date_truncation:
  -- 验证MySQL使用DATE()和DATE_FORMAT()进行时间截断
- test_postgresql_date_truncation:
  -- 验证PostgreSQL使用DATE_TRUNC()进行时间截断
- test_mysql_ansi_quotes:
  -- 验证MySQL启用ANSI_QUOTES模式支持双引号标识符
- test_postgresql_double_quotes:
  -- 验证PostgreSQL使用双引号作为标识符引用符
- test_timeout_sql_differences:
  -- 验证MySQL和PostgreSQL的超时设置SQL差异
"""

import pytest

from core.dialect_adapter import DialectAdapter


@pytest.mark.unit
def test_mysql_date_truncation():
    """
    【测试目标】
    1. 验证MySQL使用DATE()和DATE_FORMAT()进行时间截断

    【执行过程】
    1. 调用DialectAdapter.get_time_truncation_sql，指定db_type="mysql"
    2. 测试不同时间粒度（DAY, MONTH, YEAR）
    3. 验证生成的SQL包含MySQL特有的函数

    【预期结果】
    1. DAY粒度使用DATE()函数
    2. MONTH粒度使用DATE_FORMAT()函数
    3. YEAR粒度使用DATE_FORMAT()函数
    """
    col_name = "order_date"
    
    # DAY粒度
    sql_day = DialectAdapter.get_time_truncation_sql(col_name, "DAY", "mysql")
    assert "DATE(" in sql_day
    assert col_name in sql_day
    
    # MONTH粒度
    sql_month = DialectAdapter.get_time_truncation_sql(col_name, "MONTH", "mysql")
    assert "DATE_FORMAT(" in sql_month
    assert "%Y-%m-01" in sql_month
    
    # YEAR粒度
    sql_year = DialectAdapter.get_time_truncation_sql(col_name, "YEAR", "mysql")
    assert "DATE_FORMAT(" in sql_year
    assert "%Y-01-01" in sql_year


@pytest.mark.unit
def test_postgresql_date_truncation():
    """
    【测试目标】
    1. 验证PostgreSQL使用DATE_TRUNC()进行时间截断

    【执行过程】
    1. 调用DialectAdapter.get_time_truncation_sql，指定db_type="postgresql"
    2. 测试不同时间粒度（DAY, MONTH, YEAR）
    3. 验证生成的SQL包含PostgreSQL特有的函数

    【预期结果】
    1. 所有粒度都使用DATE_TRUNC()函数
    2. 第一个参数是粒度字符串（小写）
    3. 第二个参数是列名
    """
    col_name = "order_date"
    
    # DAY粒度
    sql_day = DialectAdapter.get_time_truncation_sql(col_name, "DAY", "postgresql")
    assert "DATE_TRUNC(" in sql_day
    assert "'day'" in sql_day.lower()
    assert col_name in sql_day
    
    # MONTH粒度
    sql_month = DialectAdapter.get_time_truncation_sql(col_name, "MONTH", "postgresql")
    assert "DATE_TRUNC(" in sql_month
    assert "'month'" in sql_month.lower()
    
    # YEAR粒度
    sql_year = DialectAdapter.get_time_truncation_sql(col_name, "YEAR", "postgresql")
    assert "DATE_TRUNC(" in sql_year
    assert "'year'" in sql_year.lower()


@pytest.mark.unit
def test_mysql_ansi_quotes():
    """
    【测试目标】
    1. 验证MySQL启用ANSI_QUOTES模式支持双引号标识符

    【执行过程】
    1. 调用DialectAdapter.get_session_setup_sql，指定db_type="mysql"
    2. 验证返回的SQL列表包含ANSI_QUOTES设置

    【预期结果】
    1. 返回的SQL列表包含"SET sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')"
    2. 包含超时设置SQL
    """
    sqls = DialectAdapter.get_session_setup_sql(5000, "mysql")
    
    assert len(sqls) >= 2
    assert any("ANSI_QUOTES" in sql for sql in sqls)
    assert any("max_execution_time" in sql for sql in sqls)


@pytest.mark.unit
def test_postgresql_double_quotes():
    """
    【测试目标】
    1. 验证PostgreSQL使用双引号作为标识符引用符（PyPika默认行为）

    【执行过程】
    1. 调用DialectAdapter.get_session_setup_sql，指定db_type="postgresql"
    2. 验证返回的SQL列表不包含ANSI_QUOTES设置（PostgreSQL原生支持）

    【预期结果】
    1. 返回的SQL列表不包含ANSI_QUOTES相关设置
    2. 包含statement_timeout设置
    """
    sqls = DialectAdapter.get_session_setup_sql(5000, "postgresql")
    
    assert len(sqls) >= 1
    assert not any("ANSI_QUOTES" in sql for sql in sqls)
    assert any("statement_timeout" in sql for sql in sqls)


@pytest.mark.unit
def test_timeout_sql_differences():
    """
    【测试目标】
    1. 验证MySQL和PostgreSQL的超时设置SQL差异

    【执行过程】
    1. 分别调用get_timeout_sql获取MySQL和PostgreSQL的超时SQL
    2. 验证SQL语句格式符合各自方言

    【预期结果】
    1. MySQL使用"SET max_execution_time = {timeout_ms}"
    2. PostgreSQL使用"SET statement_timeout = {timeout_ms}"
    """
    timeout_ms = 5000
    
    mysql_sql = DialectAdapter.get_timeout_sql("mysql", timeout_ms)
    assert "max_execution_time" in mysql_sql
    assert str(timeout_ms) in mysql_sql
    assert "SET" in mysql_sql
    
    postgresql_sql = DialectAdapter.get_timeout_sql("postgresql", timeout_ms)
    assert "statement_timeout" in postgresql_sql
    assert str(timeout_ms) in postgresql_sql
    assert "SET" in postgresql_sql
    
    # 验证两者不同
    assert mysql_sql != postgresql_sql


"""
Dialect Adapter Module

处理不同数据库方言的差异，主要是时间函数和会话设置。
支持 MySQL 和 PostgreSQL 两种数据库方言。
"""
from typing import List, Optional

from config.pipeline_config import SupportedDialects
from utils.log_manager import get_logger

logger = get_logger(__name__)


# ============================================================
# 时间截断函数映射表
# ============================================================
# 根据详细设计文档 3.4.3 定义
# 格式: {db_type: {grain: sql_template}}
# sql_template 中使用 {col_name} 作为占位符
TIME_TRUNCATION_MAP = {
    "mysql": {
        "DAY": "DATE({col_name})",
        "WEEK": "DATE_SUB({col_name}, INTERVAL WEEKDAY({col_name}) DAY)",
        "MONTH": "DATE_FORMAT({col_name}, '%Y-%m-01')",
        "QUARTER": "MAKEDATE(YEAR({col_name}), 1) + INTERVAL (QUARTER({col_name}) - 1) QUARTER",
        "YEAR": "DATE_FORMAT({col_name}, '%Y-01-01')",
    },
    "postgresql": {
        "DAY": "DATE_TRUNC('day', {col_name})",
        "WEEK": "DATE_TRUNC('week', {col_name})",
        "MONTH": "DATE_TRUNC('month', {col_name})",
        "QUARTER": "DATE_TRUNC('quarter', {col_name})",
        "YEAR": "DATE_TRUNC('year', {col_name})",
    },
}


class DialectAdapter:
    """
    数据库方言适配器
    
    提供统一的接口处理不同数据库的方言差异，主要包括：
    - 时间截断函数（用于时间维度分组）
    - 会话级别设置（如超时时间）
    """
    
    @staticmethod
    def get_time_truncation_sql(
        col_name: str,
        grain: str,
        db_type: str
    ) -> str:
        """
        获取时间截断 SQL 表达式
        
        根据数据库类型和时间粒度，返回对应的 SQL 表达式。
        例如：
        - MySQL DAY: DATE(order_date)
        - PostgreSQL DAY: DATE_TRUNC('day', order_date)
        
        Args:
            col_name: 列名（时间字段）
            grain: 时间粒度（DAY, WEEK, MONTH, QUARTER, YEAR）
            db_type: 数据库类型（"mysql" 或 "postgresql"）
        
        Returns:
            str: SQL 表达式字符串
        
        Raises:
            ValueError: 当 db_type 或 grain 不支持时
        """
        # 规范化输入
        db_type = db_type.lower()
        grain = grain.upper()
        
        # 验证数据库类型
        if db_type not in TIME_TRUNCATION_MAP:
            supported_types = list(TIME_TRUNCATION_MAP.keys())
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Supported types: {supported_types}"
            )
        
        # 验证时间粒度
        if grain not in TIME_TRUNCATION_MAP[db_type]:
            supported_grains = list(TIME_TRUNCATION_MAP[db_type].keys())
            raise ValueError(
                f"Unsupported time grain: {grain}. "
                f"Supported grains for {db_type}: {supported_grains}"
            )
        
        # 获取 SQL 模板
        sql_template = TIME_TRUNCATION_MAP[db_type][grain]
        
        # 替换占位符
        sql_expression = sql_template.format(col_name=col_name)
        
        logger.debug(
            "Time truncation SQL generated",
            extra={
                "col_name": col_name,
                "grain": grain,
                "db_type": db_type,
                "sql": sql_expression
            }
        )
        
        return sql_expression
    
    @staticmethod
    def get_session_setup_sql(timeout_ms: int, db_type: Optional[str] = None) -> List[str]:
        """
        获取会话级别设置 SQL 语句列表
        
        根据数据库类型，返回设置执行超时的 SQL 语句。
        这些语句需要在执行查询前执行。
        
        Args:
            timeout_ms: 超时时间（毫秒）
            db_type: 数据库类型（"mysql" 或 "postgresql"），如果为 None 则从配置读取
        
        Returns:
            List[str]: SQL 语句列表（通常只包含一条语句）
        
        Note:
            - MySQL: 使用 max_execution_time（毫秒）
            - PostgreSQL: 使用 statement_timeout（毫秒）
        """
        # 如果没有指定 db_type，从配置读取
        if db_type is None:
            from config.pipeline_config import get_pipeline_config
            config = get_pipeline_config()
            db_type = config.db_type.value
        
        # 获取对应数据库类型的超时 SQL
        timeout_sql = DialectAdapter.get_timeout_sql(db_type, timeout_ms)
        
        return [timeout_sql]
    
    @staticmethod
    def get_timeout_sql(db_type: str, timeout_ms: int) -> str:
        """
        获取指定数据库类型的超时设置 SQL
        
        Args:
            db_type: 数据库类型（"mysql" 或 "postgresql"）
            timeout_ms: 超时时间（毫秒）
        
        Returns:
            str: SQL 语句
        
        Raises:
            ValueError: 当 db_type 不支持时
        """
        db_type = db_type.lower()
        
        if db_type == "mysql":
            return f"SET max_execution_time = {timeout_ms}"
        elif db_type == "postgresql":
            return f"SET statement_timeout = {timeout_ms}"
        else:
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Supported types: mysql, postgresql"
            )


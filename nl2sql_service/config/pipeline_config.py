"""
Pipeline Configuration Module

基于 pydantic-settings 实现全局配置类，支持从 .env 文件加载配置。
遵循详细设计文档 Appendix D 的字段定义。
"""
from enum import Enum
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SupportedDialects(str, Enum):
    """支持的数据库方言枚举"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"


class PipelineConfig(BaseSettings):
    """
    流水线全局配置类
    
    使用 pydantic-settings 从环境变量和 .env 文件加载配置。
    实现单例模式，通过全局实例 pipeline_config 访问。
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # ============================================================
    # Retrieval 配置（检索相关）
    # ============================================================
    vector_search_top_k: int = Field(
        default=20,
        description="向量检索返回的 top-k 结果数量"
    )
    
    max_term_recall: int = Field(
        default=20,
        description="最大术语召回数量"
    )
    
    similarity_threshold: float = Field(
        default=0.4,
        ge=0.0,
        le=1.0,
        description="相似度阈值，用于过滤向量检索结果"
    )
    
    # ============================================================
    # Validation 配置（校验相关）
    # ============================================================
    default_limit: int = Field(
        default=100,
        gt=0,
        description="默认查询结果限制数量"
    )
    
    max_limit_cap: int = Field(
        default=1000,
        gt=0,
        description="最大查询结果限制上限"
    )
    
    # ============================================================
    # Execution 配置（执行相关）
    # ============================================================
    db_type: SupportedDialects = Field(
        default=SupportedDialects.MYSQL,
        description="数据库类型，仅支持 mysql 和 postgresql"
    )
    
    execution_timeout_ms: int = Field(
        default=5000,
        gt=0,
        description="SQL 执行超时时间（毫秒）"
    )
    
    max_result_rows: int = Field(
        default=5000,
        gt=0,
        description="最大返回结果行数"
    )
    
    # ============================================================
    # LLM 配置（大语言模型相关）
    # ============================================================
    max_llm_rows: int = Field(
        default=50,
        gt=0,
        description="Stage 6 提示词中显示的最大行数"
    )


# ============================================================
# 单例实例
# ============================================================
# 全局配置实例，在应用启动时初始化
pipeline_config: Optional[PipelineConfig] = None


def get_pipeline_config() -> PipelineConfig:
    """
    获取全局配置实例（单例模式）
    
    Returns:
        PipelineConfig: 全局配置实例
        
    Raises:
        RuntimeError: 如果配置尚未初始化
    """
    global pipeline_config
    if pipeline_config is None:
        pipeline_config = PipelineConfig()
    return pipeline_config


# 初始化全局配置实例
pipeline_config = PipelineConfig()



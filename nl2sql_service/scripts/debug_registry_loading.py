"""
语义层加载调试脚本

用于在不连接真实 Qdrant/Jina 的情况下，验证 SemanticRegistry 能否正确解析 YAML 配置文件。
"""
import asyncio
import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import yaml

# ============================================================
# 环境准备：添加项目根目录到 Python 路径
# ============================================================
# 获取 nl2sql_service 目录（脚本在 scripts/ 目录下，parent 是 nl2sql_service）
nl2sql_service_dir = Path(__file__).parent
sys.path.insert(0, str(nl2sql_service_dir))

# 现在可以导入项目模块
from core.semantic_registry import SemanticRegistry
from utils import log_manager
from utils.log_manager import get_logger

logger = get_logger(__name__)


def print_section(title: str):
    """打印分隔线"""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def print_info(message: str):
    """打印信息"""
    print(f"[INFO] {message}")


def print_success(message: str):
    """打印成功信息"""
    print(f"✅ {message}")


def print_error(message: str):
    """打印错误信息"""
    print(f"❌ {message}")


def _detect_term_type(term_id: str, term_def: Dict[str, Any]) -> str:
    """
    检测术语类型
    
    通过检查术语的 ID 前缀和特征字段来判断类型
    
    Args:
        term_id: 术语 ID
        term_def: 术语定义字典
    
    Returns:
        str: 术语类型（METRIC, DIMENSION, ENTITY, OTHER）
    """
    # 首先检查是否有显式的 type 字段
    explicit_type = term_def.get("type")
    if explicit_type:
        return explicit_type.upper()
    
    # 通过 ID 前缀判断
    term_id_upper = term_id.upper()
    if term_id_upper.startswith("METRIC_"):
        return "METRIC"
    elif term_id_upper.startswith("DIM_"):
        return "DIMENSION"
    elif term_id_upper.startswith("ENTITY_"):
        return "ENTITY"
    
    # 通过特征字段判断
    # Metrics 通常有 entity_id 和 sql_expression
    if "entity_id" in term_def and "sql_expression" in term_def:
        return "METRIC"
    
    # Dimensions 通常有 entity_id 和 data_type
    if "entity_id" in term_def and "data_type" in term_def:
        return "DIMENSION"
    
    # Entities 通常有 semantic_view
    if "semantic_view" in term_def:
        return "ENTITY"
    
    return "OTHER"


def count_terms_by_type(metadata_map: Dict[str, Any]) -> Dict[str, int]:
    """
    统计不同类型的术语数量
    
    Args:
        metadata_map: 元数据映射字典
    
    Returns:
        Dict[str, int]: 类型 -> 数量的映射
    """
    # 初始化计数器
    metric_count = 0
    dim_count = 0
    entity_count = 0
    other_count = 0
    
    # 遍历所有术语
    for term_id, term_def in metadata_map.items():
        term_type = _detect_term_type(term_id, term_def)
        
        if term_type == "METRIC":
            metric_count += 1
        elif term_type == "DIMENSION":
            dim_count += 1
        elif term_type == "ENTITY":
            entity_count += 1
        else:
            other_count += 1
    
    return {
        "METRIC": metric_count,
        "DIMENSION": dim_count,
        "ENTITY": entity_count,
        "OTHER": other_count
    }


def sample_term(metadata_map: Dict[str, Any], term_type: str) -> Dict[str, Any]:
    """
    查找第一个指定类型的术语
    
    Args:
        metadata_map: 元数据映射字典
        term_type: 术语类型（METRIC, DIMENSION, ENTITY）
    
    Returns:
        Dict[str, Any]: 术语定义，如果不存在则返回空字典
    """
    for term_id, term_def in metadata_map.items():
        detected_type = _detect_term_type(term_id, term_def)
        if detected_type == term_type:
            return term_def
    return {}


async def main():
    """主函数"""
    # 第一行：设置日志上下文，解决 KeyError: 'request_id'
    log_manager.set_request_id("debug_session")
    
    # ============================================================
    # 初始化：设置日志上下文
    # ============================================================
    print_section("开始语义层加载测试")
    
    # ============================================================
    # 步骤 1: 创建 SemanticRegistry 实例
    # ============================================================
    print_info("正在创建 SemanticRegistry 实例...")
    
    try:
        registry = await SemanticRegistry.get_instance()
        print_success("SemanticRegistry 实例创建成功")
    except Exception as e:
        print_error(f"创建 SemanticRegistry 实例失败: {e}")
        traceback.print_exc()
        return
    
    # ============================================================
    # 步骤 2: 模拟外部依赖（Qdrant 和 Jina）
    # ============================================================
    print_info("正在设置 Mock 对象（模拟 Qdrant 和 Jina）...")
    
    # 创建 Mock Qdrant 客户端
    mock_qdrant = MagicMock()
    mock_qdrant.get_collections = AsyncMock(return_value=MagicMock(collections=[]))
    mock_qdrant.retrieve = AsyncMock(return_value=[])
    mock_qdrant.upsert = AsyncMock()
    mock_qdrant.delete_collection = AsyncMock()
    mock_qdrant.create_collection = AsyncMock()
    mock_qdrant.search = AsyncMock(return_value=[])
    
    # 创建 Mock Jina 客户端（httpx.AsyncClient）
    mock_jina = MagicMock()
    # 设置 mock response 对象，包含 json() 方法
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"embedding": [0.0] * 1024}]}
    mock_response.raise_for_status = MagicMock()
    mock_jina.post = AsyncMock(return_value=mock_response)
    
    # 替换 registry 的内部客户端
    registry.qdrant_client = mock_qdrant
    registry.jina_client = mock_jina
    registry.jina_api_key = "mock_api_key"  # 设置一个假的 API Key
    
    print_success("Mock 对象设置完成，已阻断网络请求")
    
    # ============================================================
    # 步骤 3: 执行 YAML 加载
    # ============================================================
    print_info("正在加载 YAML 配置文件...")
    print_info("检测到 Mock 模式，将跳过向量数据库连接和 Embedding 生成...")
    
    yaml_path = "semantics"  # 相对于项目根目录的路径
    
    try:
        # 执行加载
        await registry.load_from_yaml(yaml_path)
        print_success("YAML 加载完成！")
    
    except FileNotFoundError as e:
        print_error(f"YAML 文件未找到: {e}")
        print_info(f"请确保 {yaml_path} 目录存在且包含 YAML 文件")
        traceback.print_exc()
        return
    
    except yaml.YAMLError as e:
        print_error(f"YAML 解析错误: {e}")
        print_info("请检查 YAML 文件的语法是否正确")
        traceback.print_exc()
        return
    
    except Exception as e:
        print_error(f"加载过程中发生未知错误: {e}")
        traceback.print_exc()
        return
    
    # ============================================================
    # 步骤 4: 健康检查与统计报告
    # ============================================================
    print_section("统计信息")
    
    # 统计各类型术语数量
    counts = count_terms_by_type(registry.metadata_map)
    
    print(f"📊 术语统计：")
    print(f"  - 指标 (Metrics): {counts['METRIC']} 个")
    print(f"  - 维度 (Dimensions): {counts['DIMENSION']} 个")
    print(f"  - 实体 (Entities): {counts['ENTITY']} 个")
    
    if counts['OTHER'] > 0:
        print(f"  - 其他类型: {counts['OTHER']} 个")
    
    print(f"\n📚 关键词索引: {len(registry.keyword_index)} 个条目")
    print(f"📦 元数据映射: {len(registry.metadata_map)} 个术语")
    
    # ============================================================
    # 步骤 5: 抽样检查
    # ============================================================
    print_section("抽样检查")
    
    # 抽取一个 Metric 示例
    if counts['METRIC'] > 0:
        metric_sample = sample_term(registry.metadata_map, "METRIC")
        if metric_sample:
            print("🔍 Metric 示例（完整属性）：")
            print("-" * 60)
            print(json.dumps(metric_sample, ensure_ascii=False, indent=2))
            print("-" * 60)
    else:
        print("⚠️  未找到 Metric 示例（可能 YAML 中未定义 Metrics）")
    
    # 抽取一个 Dimension 示例
    if counts['DIMENSION'] > 0:
        dimension_sample = sample_term(registry.metadata_map, "DIMENSION")
        if dimension_sample:
            print("\n🔍 Dimension 示例（完整属性）：")
            print("-" * 60)
            print(json.dumps(dimension_sample, ensure_ascii=False, indent=2))
            print("-" * 60)
    else:
        print("⚠️  未找到 Dimension 示例（可能 YAML 中未定义 Dimensions）")
    
    # 抽取一个 Entity 示例
    if counts['ENTITY'] > 0:
        entity_sample = sample_term(registry.metadata_map, "ENTITY")
        if entity_sample:
            print("\n🔍 Entity 示例（完整属性）：")
            print("-" * 60)
            print(json.dumps(entity_sample, ensure_ascii=False, indent=2))
            print("-" * 60)
    else:
        print("⚠️  未找到 Entity 示例（可能 YAML 中未定义 Entities）")
    
    # ============================================================
    # 步骤 6: 关键词索引检查
    # ============================================================
    if registry.keyword_index:
        print_section("关键词索引示例")
        print("📝 前 5 个关键词索引条目：")
        for i, (keyword, ids) in enumerate(list(registry.keyword_index.items())[:5]):
            print(f"  '{keyword}' -> {ids}")
        if len(registry.keyword_index) > 5:
            print(f"  ... (还有 {len(registry.keyword_index) - 5} 个条目)")
    
    # ============================================================
    # 步骤 7: 测试查找方法
    # ============================================================
    print_section("查找方法测试")
    
    # 测试 get_term
    if registry.metadata_map:
        first_term_id = list(registry.metadata_map.keys())[0]
        term = registry.get_term(first_term_id)
        if term:
            print_success(f"get_term('{first_term_id}') 返回: {term.get('name', 'N/A')}")
    
    # 测试 get_metric_def
    if counts['METRIC'] > 0:
        metric_sample = sample_term(registry.metadata_map, "METRIC")
        if metric_sample:
            metric_id = metric_sample.get("id")
            if metric_id:
                metric_def = registry.get_metric_def(metric_id)
                if metric_def:
                    print_success(f"get_metric_def('{metric_id}') 成功")
                else:
                    print_error(f"get_metric_def('{metric_id}') 返回 None")
    
    # 测试 get_dimension_def
    if counts['DIMENSION'] > 0:
        dim_sample = sample_term(registry.metadata_map, "DIMENSION")
        if dim_sample:
            dim_id = dim_sample.get("id")
            if dim_id:
                dim_def = registry.get_dimension_def(dim_id)
                if dim_def:
                    print_success(f"get_dimension_def('{dim_id}') 成功")
                else:
                    print_error(f"get_dimension_def('{dim_id}') 返回 None")
    
    # ============================================================
    # 完成
    # ============================================================
    print_section("测试完成")
    print_success("语义层加载验证通过！")
    print_info("所有 YAML 文件已成功解析并加载到内存中")


if __name__ == "__main__":
    # 运行异步主函数
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断测试")
    except Exception as e:
        print_error(f"测试脚本执行失败: {e}")
        traceback.print_exc()


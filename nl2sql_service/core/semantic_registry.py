"""
Semantic Registry Module

核心语义注册表，负责将 YAML 配置转化为内存对象和向量索引。
实现语义层的加载、检索和向量搜索功能。
"""
import asyncio
import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    CollectionStatus,
    Distance,
    Filter,
    FieldCondition,
    MatchAny,
    PointStruct,
    VectorParams,
)

from config.pipeline_config import get_pipeline_config
from core.ai_client import get_ai_client
from utils.log_manager import get_logger

logger = get_logger(__name__)

# Qdrant Collection 名称
QDRANT_COLLECTION_NAME = "semantic_terms"
# 指纹存储的 Key（存储在 Qdrant 的 payload 中）
FINGERPRINT_KEY = "_system_fingerprint"


class SemanticRegistry:
    """
    语义注册表（单例）
    
    核心职责：
    1. 加载 YAML 配置到内存（metadata_map, keyword_index）
    2. 管理向量索引（Qdrant）
    3. 提供语义检索能力（关键词匹配、向量搜索）
    4. 提供业务逻辑校验（兼容性检查、权限过滤）
    """
    
    _instance: Optional["SemanticRegistry"] = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        """初始化语义注册表（私有构造函数，通过 get_instance 获取实例）"""
        # 内存存储
        self.metadata_map: Dict[str, Any] = {}  # ID -> 定义对象
        self.keyword_index: Dict[str, List[str]] = {}  # Name/Alias -> [ID, ...]
        
        # 向量数据库客户端
        self.qdrant_client: Optional[AsyncQdrantClient] = None
        
        # 全局配置
        self.global_config: Dict[str, Any] = {}
        
        # 安全策略（从 semantic_security.yaml 加载）
        self._security_policies: Dict[str, Any] = {}
        
        # 当前指纹
        self._current_fingerprint: Optional[str] = None
        
        logger.info("SemanticRegistry instance created")
    
    @classmethod
    async def get_instance(cls) -> "SemanticRegistry":
        """
        获取单例实例（异步）
        
        Returns:
            SemanticRegistry: 单例实例
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def _calculate_yaml_fingerprint(self, yaml_path: str) -> str:
        """
        计算 YAML 文件目录的 MD5 指纹
        
        Args:
            yaml_path: YAML 文件目录路径
        
        Returns:
            str: MD5 指纹（十六进制字符串）
        """
        path = Path(yaml_path)
        if not path.exists():
            raise ValueError(f"YAML path does not exist: {yaml_path}")
        
        md5_hash = hashlib.md5()
        
        # 遍历所有 YAML 文件，按文件名排序以确保一致性
        yaml_files = sorted(path.glob("*.yaml"))
        
        for yaml_file in yaml_files:
            with open(yaml_file, "rb") as f:
                md5_hash.update(f.read())
        
        fingerprint = md5_hash.hexdigest()
        logger.debug(f"Calculated fingerprint: {fingerprint}")
        return fingerprint
    
    async def _get_stored_fingerprint(self) -> Optional[str]:
        """
        从 Qdrant 获取存储的指纹
        
        Returns:
            Optional[str]: 存储的指纹，如果不存在则返回 None
        """
        if not self.qdrant_client:
            return None
        
        try:
            # 检查 collection 是否存在
            collections = await self.qdrant_client.get_collections()
            collection_names = [c.name for c in collections.collections]
            
            if QDRANT_COLLECTION_NAME not in collection_names:
                logger.debug("Collection does not exist, no stored fingerprint")
                return None
            
            # 尝试从 collection 的 payload 中获取指纹
            # 注意：Qdrant 的 payload 存储在 point 中，我们需要一个特殊的 point 来存储系统状态
            # 这里我们使用一个特殊的 point ID (0) 来存储指纹
            try:
                result = await self.qdrant_client.retrieve(
                    collection_name=QDRANT_COLLECTION_NAME,
                    ids=[0]  # 使用 ID 0 存储系统元数据
                )
                if result and len(result) > 0 and result[0].payload:
                    fingerprint = result[0].payload.get(FINGERPRINT_KEY)
                    logger.debug(f"Retrieved stored fingerprint: {fingerprint}")
                    return fingerprint
            except Exception as e:
                logger.debug(f"Could not retrieve fingerprint from Qdrant: {e}")
                return None
        
        except Exception as e:
            logger.warning(f"Error getting stored fingerprint: {e}")
            return None
        
        return None
    
    async def _store_fingerprint(self, fingerprint: str) -> None:
        """
        将指纹存储到 Qdrant
        
        Args:
            fingerprint: 要存储的指纹
        """
        if not self.qdrant_client:
            return
        
        try:
            # 使用特殊的 point (ID=0) 存储系统元数据
            # 注意：需要与 collection 的向量维度匹配（Jina v3 是 1024 维）
            await self.qdrant_client.upsert(
                collection_name=QDRANT_COLLECTION_NAME,
                points=[
                    PointStruct(
                        id=0,
                        vector=[0.0] * 1024,  # 占位向量（Jina v3 是 1024 维）
                        payload={FINGERPRINT_KEY: fingerprint}
                    )
                ]
            )
            logger.debug(f"Stored fingerprint: {fingerprint}")
        except Exception as e:
            logger.warning(f"Error storing fingerprint: {e}")
    
    def _load_yaml_files(self, yaml_path: str) -> Dict[str, Any]:
        """
        加载所有 YAML 文件到内存（同步函数）
        
        Args:
            yaml_path: YAML 文件目录路径
        
        Returns:
            Dict[str, Any]: 合并后的配置字典
        """
        path = Path(yaml_path)
        all_data = {}
        
        # 按文件名顺序加载
        yaml_files = sorted(path.glob("*.yaml"))
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if data:
                        # 合并数据（后续文件会覆盖前面的同名键）
                        all_data.update(data)
                        logger.debug(f"Loaded YAML file: {yaml_file.name}")
            except Exception as e:
                logger.error(f"Error loading YAML file {yaml_file}: {e}")
                raise
        
        return all_data
    
    def _build_metadata_map(self, yaml_data: Dict[str, Any]) -> None:
        """
        构建 metadata_map 和 keyword_index
        
        Args:
            yaml_data: 从 YAML 加载的原始数据
        """
        self.metadata_map.clear()
        self.keyword_index.clear()
        
        # 提取全局配置
        self.global_config = yaml_data.get("global_config", {})
        
        # 提取安全策略
        self._security_policies = yaml_data.get("security", {})
        
        # 处理 metrics
        metrics = yaml_data.get("metrics", [])
        for metric in metrics:
            metric_id = metric.get("id")
            if metric_id:
                self.metadata_map[metric_id] = metric
                # 构建关键词索引
                self._add_to_keyword_index(metric_id, metric)
        
        # 处理 dimensions
        dimensions = yaml_data.get("dimensions", [])
        for dim in dimensions:
            dim_id = dim.get("id")
            if dim_id:
                self.metadata_map[dim_id] = dim
                self._add_to_keyword_index(dim_id, dim)
        
        # 处理 entities
        entities = yaml_data.get("entities", [])
        for entity in entities:
            entity_id = entity.get("id")
            if entity_id:
                self.metadata_map[entity_id] = entity
                self._add_to_keyword_index(entity_id, entity)
        
        logger.info(
            f"Built metadata_map: {len(self.metadata_map)} items, "
            f"keyword_index: {len(self.keyword_index)} entries"
        )
    
    def _add_to_keyword_index(self, term_id: str, term_def: Dict[str, Any]) -> None:
        """
        将术语添加到关键词索引
        
        Args:
            term_id: 术语 ID
            term_def: 术语定义字典
        """
        # 添加名称
        name = term_def.get("name")
        if name:
            if name not in self.keyword_index:
                self.keyword_index[name] = []
            if term_id not in self.keyword_index[name]:
                self.keyword_index[name].append(term_id)
        
        # 添加别名
        aliases = term_def.get("aliases", [])
        for alias in aliases:
            if alias not in self.keyword_index:
                self.keyword_index[alias] = []
            if term_id not in self.keyword_index[alias]:
                self.keyword_index[alias].append(term_id)
    
    async def _get_jina_embedding(self, text: str) -> List[float]:
        """
        使用 AI Client 生成文本嵌入
        
        Args:
            text: 要嵌入的文本
        
        Returns:
            List[float]: 嵌入向量
        """
        try:
            ai_client = get_ai_client()
            embeddings = await ai_client.get_embeddings(texts=[text])
            
            if not embeddings or len(embeddings) == 0:
                raise ValueError("Empty embedding response")
            
            return embeddings[0]
        
        except Exception as e:
            logger.error(f"Error getting embedding: {e}")
            raise
    
    async def _reindex_qdrant(self) -> None:
        """
        重建 Qdrant 向量索引
        
        遍历所有术语，生成 Embedding 并存储到 Qdrant。
        """
        if not self.qdrant_client:
            raise RuntimeError("Qdrant client not initialized")
        
        logger.info("Starting Qdrant reindexing...")
        
        # 删除现有 collection（如果存在）
        try:
            await self.qdrant_client.delete_collection(QDRANT_COLLECTION_NAME)
            logger.debug(f"Deleted existing collection: {QDRANT_COLLECTION_NAME}")
        except Exception:
            pass  # Collection 可能不存在
        
        # 创建新 collection
        # Jina v3 的维度是 1024
        await self.qdrant_client.create_collection(
            collection_name=QDRANT_COLLECTION_NAME,
            vectors_config=VectorParams(
                size=1024,  # jina-embeddings-v3 的维度
                distance=Distance.COSINE
            )
        )
        logger.info(f"Created collection: {QDRANT_COLLECTION_NAME}")
        
        # 为每个术语生成 Embedding 并存储
        points = []
        point_id = 1  # 从 1 开始，0 用于存储系统元数据
        
        for term_id, term_def in self.metadata_map.items():
            # 构建搜索文本（名称 + 描述）
            name = term_def.get("name", "")
            description = term_def.get("description", "")
            search_text = f"{name} {description}".strip()
            
            if not search_text:
                continue
            
            try:
                # 生成 Embedding
                embedding = await self._get_jina_embedding(search_text)
                
                # 构建 Point
                points.append(
                    PointStruct(
                        id=point_id,
                        vector=embedding,
                        payload={
                            "id": term_id,
                            "name": name,
                            "type": term_def.get("type", "UNKNOWN")
                        }
                    )
                )
                point_id += 1
                
                # 批量插入（每 100 个一批）
                if len(points) >= 100:
                    await self.qdrant_client.upsert(
                        collection_name=QDRANT_COLLECTION_NAME,
                        points=points
                    )
                    logger.debug(f"Inserted {len(points)} points into Qdrant")
                    points = []
            
            except Exception as e:
                logger.warning(f"Error processing term {term_id}: {e}")
                continue
        
        # 插入剩余的点
        if points:
            await self.qdrant_client.upsert(
                collection_name=QDRANT_COLLECTION_NAME,
                points=points
            )
            logger.debug(f"Inserted final {len(points)} points into Qdrant")
        
        logger.info(f"Reindexing completed. Total points: {point_id - 1}")
    
    async def load_from_yaml(self, yaml_path: str = "semantics") -> None:
        """
        从 YAML 文件加载语义配置
        
        流程：
        1. 计算 YAML 文件的 MD5 指纹
        2. 检查 Qdrant 中存储的指纹
        3. 如果指纹一致：快速路径（仅加载到内存）
        4. 如果指纹不一致：重新索引（加载 + 生成 Embedding + 重建 Qdrant）
        
        Args:
            yaml_path: YAML 文件目录路径，默认为 "semantics"
        """
        logger.info(f"Loading semantic registry from: {yaml_path}")
        
        # Step 1: 计算指纹
        current_fingerprint = self._calculate_yaml_fingerprint(yaml_path)
        self._current_fingerprint = current_fingerprint
        
        # Step 2: 检查存储的指纹
        stored_fingerprint = await self._get_stored_fingerprint()
        
        # Step 3: 分支处理
        if stored_fingerprint == current_fingerprint:
            # 快速路径：指纹一致，仅加载 YAML 到内存
            logger.info("Fast Path: Fingerprint matches, loading YAML only")
            yaml_data = await asyncio.to_thread(self._load_yaml_files, yaml_path)
            self._build_metadata_map(yaml_data)
        else:
            # 重新索引：指纹不一致，需要重建向量索引
            logger.info("Re-indexing: Fingerprint mismatch, rebuilding vector index")
            
            # 加载 YAML
            yaml_data = await asyncio.to_thread(self._load_yaml_files, yaml_path)
            self._build_metadata_map(yaml_data)
            
            # 重建 Qdrant 索引
            await self._reindex_qdrant()
            
            # 存储新指纹
            await self._store_fingerprint(current_fingerprint)
        
        logger.info("Semantic registry loaded successfully")
    
    def _init_clients(self) -> None:
        """初始化 Qdrant 客户端"""
        # 初始化 Qdrant 客户端
        qdrant_host = os.getenv("QDRANT_HOST", "localhost")
        qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        qdrant_api_key = os.getenv("QDRANT_API_KEY")
        
        qdrant_kwargs = {
            "host": qdrant_host,
            "port": qdrant_port,
        }
        if qdrant_api_key:
            qdrant_kwargs["api_key"] = qdrant_api_key
        
        self.qdrant_client = AsyncQdrantClient(**qdrant_kwargs)
        logger.info(f"Initialized Qdrant client: {qdrant_host}:{qdrant_port}")
    
    async def initialize(self, yaml_path: str = "semantics") -> None:
        """
        初始化语义注册表（完整流程）
        
        Args:
            yaml_path: YAML 文件目录路径
        """
        self._init_clients()
        await self.load_from_yaml(yaml_path)
    
    # ============================================================
    # 基础查询方法（同步/内存）
    # ============================================================
    
    def get_term(self, term_id: str) -> Optional[Dict[str, Any]]:
        """
        通用获取方法，返回 Metric/Dimension/Entity 等对象
        
        Args:
            term_id: 术语 ID
        
        Returns:
            Optional[Dict[str, Any]]: 术语定义，如果不存在则返回 None
        """
        return self.metadata_map.get(term_id)
    
    def get_metric_def(self, metric_id: str) -> Optional[Dict[str, Any]]:
        """
        获取指标定义
        
        Args:
            metric_id: 指标 ID
        
        Returns:
            Optional[Dict[str, Any]]: 指标定义，如果不存在或不是 Metric 则返回 None
        """
        term = self.get_term(metric_id)
        if term and "metric_type" in term:
            return term
        return None
    
    def get_dimension_def(self, dimension_id: str) -> Optional[Dict[str, Any]]:
        """
        获取维度定义
        
        Args:
            dimension_id: 维度 ID
        
        Returns:
            Optional[Dict[str, Any]]: 维度定义，如果不存在或不是 Dimension 则返回 None
        """
        term = self.get_term(dimension_id)
        if term and term.get("id", "").startswith("DIM_"):
            return term
        return None
    
    def get_entity_def(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """
        获取实体定义
        
        Args:
            entity_id: 实体 ID
        
        Returns:
            Optional[Dict[str, Any]]: 实体定义，如果不存在或不是 Entity 则返回 None
        """
        term = self.get_term(entity_id)
        if term and term.get("type") == "ENTITY":
            return term
        return None
    
    def get_relation(self, from_entity: str, to_entity: str) -> Optional[Dict[str, Any]]:
        """
        查找实体间的关联关系（Join 路径）
        
        Args:
            from_entity: 源实体 ID
            to_entity: 目标实体 ID
        
        Returns:
            Optional[Dict[str, Any]]: 关联关系定义，如果不存在则返回 None
        """
        # TODO: 实现实体关系查找逻辑
        # 这需要从 YAML 中读取 relations 或 joins 配置
        logger.warning(f"get_relation not yet implemented: {from_entity} -> {to_entity}")
        return None
    
    # ============================================================
    # 业务逻辑校验方法
    # ============================================================
    
    def check_compatibility(self, metric_id: str, dimension_id: str) -> bool:
        """
        检查指标和维度是否兼容（属于同一个 Entity）
        
        Args:
            metric_id: 指标 ID
            dimension_id: 维度 ID
        
        Returns:
            bool: 如果兼容则返回 True
        """
        m_def = self.get_metric_def(metric_id)
        d_def = self.get_dimension_def(dimension_id)
        
        if not m_def or not d_def:
            return False
        
        # 核心逻辑：比较 Entity ID 是否一致
        m_entity = m_def.get("entity_id")
        d_entity = d_def.get("entity_id")
        
        return m_entity is not None and m_entity == d_entity
    
    def get_allowed_ids(self, role_id: str) -> Set[str]:
        """
        根据角色获取允许访问的 ID 集合
        
        Args:
            role_id: 角色 ID
        
        Returns:
            Set[str]: 允许访问的术语 ID 集合
        """
        # TODO: 从 _security_policies 中读取角色权限
        # 当前返回所有 ID（无权限限制）
        logger.warning(f"get_allowed_ids not yet fully implemented for role: {role_id}")
        return set(self.metadata_map.keys())
    
    def get_rls_policies(self, role_id: str, entity_id: str) -> List[str]:
        """
        获取行级安全策略（RLS）SQL 片段
        
        Args:
            role_id: 角色 ID
            entity_id: 实体 ID
        
        Returns:
            List[str]: RLS SQL 片段列表
        """
        # TODO: 从 _security_policies 中读取 RLS 策略
        logger.warning(f"get_rls_policies not yet fully implemented: role={role_id}, entity={entity_id}")
        return []
    
    # ============================================================
    # 向量搜索方法（异步）
    # ============================================================
    
    async def search_similar_terms(
        self,
        query: str,
        allowed_ids: Optional[List[str]] = None,
        top_k: int = 20
    ) -> List[Tuple[str, float]]:
        """
        搜索相似的术语（向量搜索）
        
        Args:
            query: 查询文本
            allowed_ids: 允许的 ID 列表（用于权限过滤），如果为 None 则不过滤
            top_k: 返回的 top-k 结果数量
        
        Returns:
            List[Tuple[str, float]]: [(term_id, score), ...] 列表，按相似度降序排列
        """
        if not self.qdrant_client:
            raise RuntimeError("Qdrant client not initialized")
        
        try:
            # 生成查询向量
            query_embedding = await self._get_jina_embedding(query)
            
            # 构建过滤条件
            qdrant_filter = None
            if allowed_ids:
                qdrant_filter = Filter(
                    must=[
                        FieldCondition(
                            key="id",
                            match=MatchAny(any=allowed_ids)
                        )
                    ]
                )
            
            # 搜索 Qdrant
            search_results = await self.qdrant_client.search(
                collection_name=QDRANT_COLLECTION_NAME,
                query_vector=query_embedding,
                query_filter=qdrant_filter,
                limit=top_k
            )
            
            # 提取结果
            results = [
                (point.payload.get("id"), point.score)
                for point in search_results
                if point.payload and "id" in point.payload
            ]
            
            logger.debug(
                f"Vector search completed: query='{query}', results={len(results)}"
            )
            
            return results
        
        except Exception as e:
            logger.error(f"Error in vector search: {e}")
            raise


# ============================================================
# 全局单例访问函数
# ============================================================
_semantic_registry: Optional[SemanticRegistry] = None


async def get_semantic_registry() -> SemanticRegistry:
    """
    获取语义注册表单例实例
    
    Returns:
        SemanticRegistry: 语义注册表实例
    """
    return await SemanticRegistry.get_instance()


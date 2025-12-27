"""
Semantic Registry Module

核心语义注册表，负责将 YAML 配置转化为内存对象和向量索引。
实现语义层的加载、检索和向量搜索功能。
"""
import asyncio
import hashlib
import inspect
import os
import re
import time
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
# 默认本地存储路径（项目根目录下的 qdrant_data/）
# 计算方式：从当前文件 (semantic_registry.py) 向上三级到达项目根目录
DEFAULT_STORAGE_PATH = Path(__file__).parent.parent.parent / "qdrant_data"


class SecurityConfigError(Exception):
    """安全配置错误（加载/解析失败等），应视为 500 配置错误。"""


class SecurityPolicyNotFound(Exception):
    """角色未配置安全策略（fail-closed），应映射为 403。"""

    def __init__(self, role_id: str):
        super().__init__(f"Security policy not found for role_id={role_id}")
        self.role_id = role_id


class SemanticConfigurationError(Exception):
    """
    语义配置错误（时间窗口/全局配置等加载后解析失败）。

    需要被上层映射为稳定错误码 CONFIGURATION_ERROR。
    """

    code = "CONFIGURATION_ERROR"

    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class SemanticRegistry:
    """
    SemanticRegistry（单例）
    
    核心职责：
    1. 加载 YAML 配置到内存（metadata_map, keyword_index）
    2. 管理向量索引（Qdrant）
    3. 提供语义检索能力（关键词匹配、向量搜索）
    4. 提供业务逻辑校验（兼容性检查、权限过滤）
    """
    
    _instance: Optional["SemanticRegistry"] = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        """初始化 SemanticRegistry（私有构造函数，通过 get_instance 获取实例）"""
        # 内存存储
        self.metadata_map: Dict[str, Any] = {}  # ID -> 定义对象
        self.keyword_index: Dict[str, List[str]] = {}  # Name/Alias -> [ID, ...]
        
        # 向量数据库客户端
        self.qdrant_client: Optional[AsyncQdrantClient] = None
        
        # 全局配置
        self.global_config: Dict[str, Any] = {}
        
        # 安全策略（从 semantic_security.yaml 加载）
        self._security_policies: Dict[str, Any] = {}
        # role_id -> policy dict（来自 security.role_policies）
        self._role_policy_map: Dict[str, Dict[str, Any]] = {}
        # role_id -> allowed_ids cache
        self._allowed_ids_cache: Dict[str, Set[str]] = {}
        
        # 当前指纹
        self._current_fingerprint: Optional[str] = None
        
        # 临时 Qdrant 路径（仅当 fallback 到 instance_{pid} 时设置，用于退出时清理）
        self._temp_qdrant_path: Optional[Path] = None
        
        logger.info("SemanticRegistry instance created")
    
    async def close(self) -> None:
        """
        关闭 SemanticRegistry 并清理资源
        
        主要清理 Qdrant 客户端连接，避免资源泄漏和文件锁未关闭。
        如果使用了临时 fallback 目录（instance_{pid}），会在关闭时自动清理。
        """
        if self.qdrant_client:
            try:
                # AsyncQdrantClient 需要调用 close() 方法
                # 注意：AsyncQdrantClient 可能没有 close/aclose 方法，需要检查
                if hasattr(self.qdrant_client, "close"):
                    close_method = getattr(self.qdrant_client, "close")
                    res = close_method()
                    if inspect.isawaitable(res):
                        await res
                elif hasattr(self.qdrant_client, "aclose"):
                    aclose_method = getattr(self.qdrant_client, "aclose")
                    res = aclose_method()
                    if inspect.isawaitable(res):
                        await res
                # 如果都没有，尝试访问 _client 属性（内部 HTTP 客户端）
                elif hasattr(self.qdrant_client, "_client"):
                    client = getattr(self.qdrant_client, "_client")
                    if hasattr(client, "close"):
                        close_method = getattr(client, "close")
                        res = close_method()
                        if inspect.isawaitable(res):
                            await res
                logger.debug("Qdrant client closed successfully")
            except Exception as e:
                logger.warning(f"Error closing Qdrant client: {e}")
            finally:
                self.qdrant_client = None
        
        # B2: 清理临时 fallback 目录（仅当使用了 instance_{pid} 时）
        if self._temp_qdrant_path and self._temp_qdrant_path.exists():
            try:
                # C: 安全删除 guard - 只允许删除真正的 fallback instance 目录
                temp_path = self._temp_qdrant_path
                
                # 计算 fallback 根目录（必须与 _init_clients() 中创建 fallback_path 的逻辑一致）
                store_path_str = os.getenv("VECTOR_STORE_PATH")
                root = Path(store_path_str) if store_path_str else DEFAULT_STORAGE_PATH
                
                # 校验 1: 目录名必须严格匹配 instance_<pid数字> 格式（使用正则确保完整匹配）
                is_valid_name = bool(re.match(r"^instance_\d+$", temp_path.name))
                
                # 校验 2: 目录必须位于 fallback 根目录之下（使用强校验：Path.is_relative_to）
                is_relative = False
                try:
                    is_relative = temp_path.resolve().is_relative_to(root.resolve())
                except (AttributeError, ValueError):
                    # Python < 3.11 或路径不相关，使用备用强校验方法
                    try:
                        resolved_temp = temp_path.resolve()
                        resolved_root = root.resolve()
                        is_relative = resolved_root in resolved_temp.parents or resolved_temp == resolved_root
                    except Exception:
                        pass
                
                if is_valid_name and is_relative:
                    # 通过双重校验，安全删除
                    import shutil
                    shutil.rmtree(temp_path, ignore_errors=True)
                    logger.debug(f"Cleaned up temporary Qdrant directory: {temp_path}")
                else:
                    # 校验失败，不删除并记录警告
                    logger.warning(
                        "Refused to delete Qdrant directory: safety check failed",
                        extra={
                            "temp_path": str(temp_path),
                            "root": str(root),
                            "is_valid_name": is_valid_name,
                            "is_relative": is_relative,
                        },
                    )
            except Exception as e:
                logger.warning(f"Error cleaning up temporary Qdrant directory {self._temp_qdrant_path}: {e}")
            finally:
                self._temp_qdrant_path = None
    
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
        loaded_files = []
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if data:
                        # 合并数据（后续文件会覆盖前面的同名键）
                        all_data.update(data)
                        loaded_files.append(yaml_file.stem)  # 只保存文件名（不含扩展名）
            except Exception as e:
                logger.error(f"Error loading YAML file {yaml_file}: {e}")
                raise
        
        # 合并打印：一次性显示所有加载的文件
        if loaded_files:
            logger.debug(f"已加载 YAML 配置 ({', '.join(loaded_files)})")

        # 兼容两种写法，并对浅合并后的结果做一次安全配置归一化：
        # - security: { role_policies: [...] }
        # - role_policies: [...]
        # - 两者同时存在时合并 role_policies，避免覆盖丢失
        try:
            security = all_data.get("security") if isinstance(all_data.get("security"), dict) else {}
            top_role_policies = all_data.get("role_policies")
            if top_role_policies is not None:
                if "role_policies" in security and isinstance(security.get("role_policies"), list):
                    if isinstance(top_role_policies, list):
                        security["role_policies"] = security["role_policies"] + top_role_policies
                else:
                    if isinstance(top_role_policies, list):
                        security["role_policies"] = top_role_policies
            if security:
                all_data["security"] = security
        except Exception as e:
            # 归一化失败应视为配置错误（不要伪装成 403）
            raise SecurityConfigError(f"Failed to normalize security config: {e}") from e
        
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
        self._security_policies = yaml_data.get("security", {}) if isinstance(yaml_data.get("security", {}), dict) else {}
        self._rebuild_security_indexes()
        
        # 提取 enums 和 logical_filters（用于 Stage4 逻辑过滤器展开）
        self._enums = yaml_data.get("enums", [])
        self._logical_filters = yaml_data.get("logical_filters", [])
        
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
        
        # =========================================================
        # 处理实体定义 (Entities)
        # 依据 semantic_core.yaml 结构，此列表下的所有对象均为实体
        # =========================================================
        entities = yaml_data.get("entities", [])
        for entity in entities:
            entity_id = entity.get("id")

            # 【防御性检查 1】: 实体必须有 ID
            if not entity_id:
                raise SemanticConfigurationError(
                    "Entity definition missing required 'id' field",
                    details={"entity_content": entity}
                )

            # 【防御性检查 2 - 铁律执行】: 必须遵守全局命名规范
            # 既然规范已定，就在加载时强制执行，防止脏数据进入系统
            if not entity_id.startswith("ENT_"):
                raise SemanticConfigurationError(
                    f"Violation of Naming Convention: Entity ID '{entity_id}' must start with 'ENT_'.",
                    details={"entity_id": entity_id, "source": "semantic_core.yaml"}
                )

            # 【数据标准化 - Bug 修复】
            # 使用 copy() 防止修改原始 yaml_data (如果该数据被缓存或复用)
            entity_def = entity.copy()
            
            # 显式注入内部多态标识 'type'
            # 即使 YAML 中有 'entity_type' (FACT/DIM), 内部路由逻辑依赖的是 type="ENTITY"
            entity_def["type"] = "ENTITY"

            # 【注册与索引】
            self.metadata_map[entity_id] = entity_def
            self._add_to_keyword_index(entity_id, entity_def)
        
        # =========================================================
        # 处理通用词汇表 (Common Vocabulary)
        # =========================================================
        common_vocab_list = []
        if isinstance(self.global_config, dict):
            common_vocab_list = self.global_config.get("common_vocabulary", []) or []
        
        if isinstance(common_vocab_list, list):
            for vocab_item in common_vocab_list:
                if not isinstance(vocab_item, dict):
                    continue
                
                term = vocab_item.get("term")
                if not term:
                    continue
                
                # 保存 YAML 的 type 到 vocab_type，设置内部 type 为 VOCABULARY
                vocab_type = vocab_item.get("type", "UNKNOWN")
                vocab_value = vocab_item.get("value")
                
                # 生成稳定且不冲突的 vocab_id
                # 规则：VOCAB_{VOCAB_TYPE} 或 VOCAB_{VOCAB_TYPE}_{VALUE}
                # 全部转为大写字符串；非字母数字用下划线替换
                vocab_id_base = f"VOCAB_{vocab_type.upper().replace('-', '_').replace(' ', '_')}"
                if vocab_value is not None:
                    # 将 value 转换为字符串并规范化
                    value_str = str(vocab_value).upper().replace('-', '_').replace(' ', '_')
                    # 移除非字母数字字符（保留下划线）
                    value_str = re.sub(r'[^A-Z0-9_]', '_', value_str)
                    vocab_id = f"{vocab_id_base}_{value_str}"
                else:
                    vocab_id = vocab_id_base
                
                # 检查冲突
                if vocab_id in self.metadata_map:
                    existing_term = self.metadata_map[vocab_id].get("term", "")
                    raise SemanticConfigurationError(
                        f"Duplicate vocabulary ID: {vocab_id}. "
                        f"Multiple vocabulary items share the same type={vocab_type} and value={vocab_value}",
                        details={
                            "vocab_id": vocab_id,
                            "vocab_type": vocab_type,
                            "value": vocab_value,
                            "existing_term": existing_term,
                            "conflicting_term": term
                        }
                    )
                
                # 构建词汇定义对象
                vocab_def = vocab_item.copy()
                vocab_def["vocab_type"] = vocab_type  # 保存 YAML 的 type
                vocab_def["type"] = "VOCABULARY"  # 设置内部 type
                vocab_def["id"] = vocab_id  # 设置生成的 ID
                vocab_def["name"] = term  # 将 term 映射为 name，让 _add_to_keyword_index 能处理
                
                # 注册到 metadata_map
                self.metadata_map[vocab_id] = vocab_def
                
                # 添加到关键词索引（term 和所有 aliases）
                self._add_to_keyword_index(vocab_id, vocab_def)
        
        logger.info(
            f"Built metadata_map: {len(self.metadata_map)} items, "
            f"keyword_index: {len(self.keyword_index)} entries"
        )

    def _rebuild_security_indexes(self) -> None:
        """重建安全索引缓存（role_id -> policy）与 allowed_ids 缓存。"""
        self._role_policy_map.clear()
        self._allowed_ids_cache.clear()

        role_policies = None
        if isinstance(self._security_policies, dict):
            role_policies = self._security_policies.get("role_policies")
        if role_policies is None:
            return
        if not isinstance(role_policies, list):
            raise SecurityConfigError("security.role_policies must be a list")

        for policy in role_policies:
            if not isinstance(policy, dict):
                continue
            role_id = policy.get("role_id")
            if not role_id:
                continue
            # 同 role_id 多条策略：后者覆盖前者（显式更新更优先）
            self._role_policy_map[str(role_id)] = policy
    
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
        
        logger.info("开始重建向量索引...")
        
        # 删除现有 collection（如果存在）
        try:
            await self.qdrant_client.delete_collection(QDRANT_COLLECTION_NAME)
            logger.debug(f"已删除旧集合: {QDRANT_COLLECTION_NAME}")
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
        logger.info(f"已创建新集合: {QDRANT_COLLECTION_NAME}")
        
        # 为每个术语生成 Embedding 并存储
        points = []
        point_id = 1  # 从 1 开始，0 用于存储系统元数据
        
        for term_id, term_def in self.metadata_map.items():
            term_type = term_def.get("type", "UNKNOWN")
            
            # 构建搜索文本（根据类型不同采用不同策略）
            # 同时确保 payload 使用的 name 字段已定义
            if term_type == "VOCABULARY":
                # vocabulary: term + aliases
                term = term_def.get("term") or term_def.get("name", "")
                aliases = term_def.get("aliases", [])
                alias_text = " ".join(aliases) if isinstance(aliases, list) else ""
                search_text = f"{term} {alias_text}".strip()
                # 对于 vocabulary，payload 使用 term 作为 name
                payload_name = term
            else:
                # 其他类型：name + description
                name = term_def.get("name", "")
                description = term_def.get("description", "")
                search_text = f"{name} {description}".strip()
                # 对于其他类型，payload 使用 name
                payload_name = name
            
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
                            "name": payload_name,
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
                    logger.debug(f"已插入 {len(points)} 个向量点")
                    points = []
            
            except Exception as e:
                logger.warning(f"处理术语失败 {term_id}: {e}")
                continue
        
        # 插入剩余的点
        if points:
            await self.qdrant_client.upsert(
                collection_name=QDRANT_COLLECTION_NAME,
                points=points
            )
            logger.debug(f"已插入最后 {len(points)} 个向量点")
        
        logger.info(f"向量索引重建完成 | 总数: {point_id - 1}")
    
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
        logger.info(f"加载 SemanticRegistry | 路径: {yaml_path}")
        
        # Step 1: 计算指纹
        current_fingerprint = self._calculate_yaml_fingerprint(yaml_path)
        self._current_fingerprint = current_fingerprint
        
        # Step 2: 检查存储的指纹
        stored_fingerprint = await self._get_stored_fingerprint()
        
        # Step 3: 分支处理
        # 检查是否在离线模式（NO_NETWORK=1）
        no_network = os.getenv("NO_NETWORK", "").lower() in ("1", "true", "yes")
        
        if stored_fingerprint == current_fingerprint:
            # 快速路径：指纹一致，仅加载 YAML 到内存
            logger.info("快速加载 | 指纹匹配，跳过向量索引")
            yaml_data = await asyncio.to_thread(self._load_yaml_files, yaml_path)
            self._build_metadata_map(yaml_data)
        elif no_network:
            # 离线模式：跳过向量索引重建，仅加载 YAML 到内存
            logger.info("离线模式 | 跳过向量索引重建（NO_NETWORK=1），仅加载 YAML 配置")
            yaml_data = await asyncio.to_thread(self._load_yaml_files, yaml_path)
            self._build_metadata_map(yaml_data)
            # 注意：离线模式下不存储指纹，因为索引未重建
        else:
            # 重新索引：指纹不一致，需要重建向量索引
            logger.info("重建索引 | 指纹不匹配，重建向量索引")
            
            # 加载 YAML
            yaml_data = await asyncio.to_thread(self._load_yaml_files, yaml_path)
            self._build_metadata_map(yaml_data)
            
            # 重建 Qdrant 索引
            await self._reindex_qdrant()
            
            # 存储新指纹
            await self._store_fingerprint(current_fingerprint)
        
        logger.info("SemanticRegistry 加载完成")
    
    def _init_clients(self) -> None:
        """初始化 Qdrant 客户端
        
        支持三种模式：
        - local: 本地文件系统存储（默认）
        - memory: 内存存储（临时，进程退出后丢失）
        - remote: 远程 Qdrant 服务
        
        默认行为：如果未设置 VECTOR_STORE_MODE 或值为无效，一律使用 local 模式。
        Remote 模式仅在明确设置 VECTOR_STORE_MODE=remote 时启用。
        
        注意：在离线测试模式（NO_NETWORK=1）下，强制使用 memory 模式，避免文件锁。
        """
        # 检查是否在离线模式，如果是，强制使用 memory 模式
        no_network = os.getenv("NO_NETWORK", "").lower() in ("1", "true", "yes")
        if no_network:
            mode = "memory"
            logger.debug("Offline mode detected (NO_NETWORK=1), forcing VECTOR_STORE_MODE=memory")
        else:
            mode = os.getenv("VECTOR_STORE_MODE", "local").lower()
        
        if mode == "memory":
            # Memory 模式：使用内存存储
            self.qdrant_client = AsyncQdrantClient(location=":memory:")
            logger.info("Initialized Qdrant client in MEMORY mode")
        
        elif mode == "remote":
            # Remote 模式：连接到远程 Qdrant 服务
            qdrant_api_key = os.getenv("QDRANT_API_KEY")
            qdrant_kwargs = {}
            
            if qdrant_api_key:
                qdrant_kwargs["api_key"] = qdrant_api_key
            
            # 优先使用 QDRANT_URL（如果设置）
            qdrant_url = os.getenv("QDRANT_URL")
            if qdrant_url:
                self.qdrant_client = AsyncQdrantClient(url=qdrant_url, **qdrant_kwargs)
                logger.info(f"Initialized Qdrant client in REMOTE mode, url: {qdrant_url}")
            else:
                # 回退到 Host + Port 方式（向后兼容）
                qdrant_host = os.getenv("QDRANT_HOST", "localhost")
                qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
                qdrant_kwargs["host"] = qdrant_host
                qdrant_kwargs["port"] = qdrant_port
                self.qdrant_client = AsyncQdrantClient(**qdrant_kwargs)
                logger.info(f"Initialized Qdrant client in REMOTE mode, host: {qdrant_host}:{qdrant_port}")
        
        else:
            # Local 模式（默认）：使用本地文件系统存储
            # 如果 mode 不是 "memory" 或 "remote"，一律使用 local 模式
            store_path_str = os.getenv("VECTOR_STORE_PATH")
            if store_path_str:
                store_path = Path(store_path_str)
            else:
                store_path = DEFAULT_STORAGE_PATH
            
            # 确保目录存在
            store_path.mkdir(parents=True, exist_ok=True)

            # 可选：为开发场景（如 uvicorn --reload 多进程）启用"每进程隔离"存储目录，避免文件锁冲突
            isolate_per_process = os.getenv("VECTOR_STORE_ISOLATE_PER_PROCESS", "").lower() in {"1", "true", "yes", "on"}
            if isolate_per_process:
                store_path = store_path / f"instance_{os.getpid()}"
                store_path.mkdir(parents=True, exist_ok=True)

            # B1: 短暂重试窗口（最多 3 次，每次 0.3s，总耗时 <= 1s）
            max_retries = 3
            retry_delay = 0.3
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    self.qdrant_client = AsyncQdrantClient(path=str(store_path))
                    logger.info(f"Initialized Qdrant client in LOCAL mode, storage path: {store_path}")
                    break  # 成功，退出重试循环
                except Exception as e:
                    last_exception = e
                    msg = str(e)
                    lock_like = ("already accessed by another instance" in msg.lower()) or ("alreadylocked" in msg.lower())
                    
                    if not lock_like:
                        # 非锁冲突错误，直接抛出
                        raise
                    
                    # 锁冲突：如果不是最后一次重试，等待后重试
                    if attempt < max_retries - 1:
                        logger.debug(
                            f"Qdrant 路径被锁定，重试中 ({attempt + 1}/{max_retries})",
                            extra={"path": str(store_path), "error": msg}
                        )
                        time.sleep(retry_delay)
                    else:
                        # 最后一次重试也失败，进入 fallback
                        logger.warning(
                            f"Qdrant 路径被锁定，重试 {max_retries} 次后仍失败，切换到进程隔离目录",
                            extra={
                                "original_path": str(store_path),
                                "error": msg,
                                "retries": max_retries,
                            },
                        )
                        # B2: 自动降级到进程隔离目录，并记录临时路径用于清理
                        fallback_path = (Path(store_path_str) if store_path_str else DEFAULT_STORAGE_PATH) / f"instance_{os.getpid()}"
                        fallback_path.mkdir(parents=True, exist_ok=True)
                        self._temp_qdrant_path = fallback_path  # 记录临时路径，退出时清理
                        self.qdrant_client = AsyncQdrantClient(path=str(fallback_path))
                        logger.info(f"Qdrant 初始化 | 本地模式（进程隔离）| {fallback_path}")
    
    async def initialize(self, yaml_path: str = "semantics") -> None:
        """
        初始化 SemanticRegistry（完整流程）
        
        Args:
            yaml_path: YAML 文件目录路径
        """
        await asyncio.to_thread(self._init_clients)
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
    
    def get_logical_filter_def(self, logical_filter_id: str) -> Optional[Dict[str, Any]]:
        """
        获取逻辑过滤器定义（用于 Stage4 展开为 SQL WHERE 条件）
        
        Args:
            logical_filter_id: 逻辑过滤器 ID（如 LF_REVENUE_VALID_ORDER）
        
        Returns:
            Optional[Dict[str, Any]]: 逻辑过滤器定义，包含 filters 列表
        """
        if not hasattr(self, '_logical_filters') or not isinstance(self._logical_filters, list):
            return None
        
        for lf in self._logical_filters:
            if isinstance(lf, dict) and lf.get("id") == logical_filter_id:
                return lf
        return None
    
    def get_enum_values(self, enum_id: str) -> Optional[List[str]]:
        """
        获取枚举值集合（用于 IN_SET 操作符展开）
        
        Args:
            enum_id: 枚举 ID（如 STATUS_VALID_FOR_REVENUE）
        
        Returns:
            Optional[List[str]]: 枚举值列表，如 ["Resolved", "Shipped"]
        """
        if not hasattr(self, '_enums') or not isinstance(self._enums, list):
            return None
        
        for enum_def in self._enums:
            if isinstance(enum_def, dict) and enum_def.get("id") == enum_id:
                values = enum_def.get("values")
                if isinstance(values, list):
                    return values
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
        # fail-closed：安全配置缺失/异常 => 500；role 未配置 => 403
        if not self._security_policies:
            raise SecurityConfigError("Security config is not loaded (missing 'security')")

        # 索引为空但配置存在：容错重建一次
        if not self._role_policy_map:
            self._rebuild_security_indexes()

        if not self._role_policy_map:
            raise SecurityConfigError("Security config missing 'role_policies'")

        if role_id in self._allowed_ids_cache:
            return self._allowed_ids_cache[role_id]

        policy = self._role_policy_map.get(role_id)
        if not policy:
            raise SecurityPolicyNotFound(role_id)

        policy_id = policy.get("policy_id")
        scopes = policy.get("scopes", {}) if isinstance(policy.get("scopes", {}), dict) else {}

        domain_access = set(scopes.get("domain_access", []) or [])
        entity_scope = scopes.get("entity_scope", []) or []
        dimension_scope = scopes.get("dimension_scope", []) or []
        metric_scope = scopes.get("metric_scope", []) or []

        # 统一为字符串列表
        def _as_str_list(x: Any) -> List[str]:
            if x is None:
                return []
            if isinstance(x, list):
                return [str(i) for i in x if i is not None]
            return [str(x)]

        domain_access_list = _as_str_list(list(domain_access))
        domain_access = set(domain_access_list)
        # Critical Fix: 强制追加 COMMON 域，确保通用维度（如时间、地理位置）对所有用户可见
        domain_access = domain_access | {"COMMON"}
        entity_scope = _as_str_list(entity_scope)
        dimension_scope = _as_str_list(dimension_scope)
        metric_scope = _as_str_list(metric_scope)

        domain_all = "ALL" in domain_access

        def _term_type(term_id: str) -> str:
            if term_id.startswith("METRIC_"):
                return "METRIC"
            if term_id.startswith("DIM_"):
                return "DIM"
            if term_id.startswith("ENT_"):
                return "ENT"
            if term_id.startswith("VOCAB_"):
                return "VOCABULARY"
            return "OTHER"

        def _domain_allowed(term_domain: Optional[str]) -> bool:
            if domain_all:
                return True
            if not term_domain:
                return False
            return term_domain in domain_access

        # scope helpers
        metric_explicit_ids = {s for s in metric_scope if s.startswith("METRIC_")}
        dim_explicit_ids = {s for s in dimension_scope if s.startswith("DIM_")}
        ent_explicit_ids = {s for s in entity_scope if s.startswith("ENT_")}

        metric_has_all = "ALL" in metric_scope
        dim_has_all = "ALL" in dimension_scope
        ent_has_all = "ALL" in entity_scope

        # 形如 "HR_ALL" / "HR_BASE" / "SALES_ALL" ...
        metric_domain_rules = {s for s in metric_scope if "_" in s and not s.startswith("METRIC_")}
        # 形如 "HR_"（以 "_" 结尾）
        dim_domain_families = {s for s in dimension_scope if s.endswith("_") and not s.startswith("DIM_")}
        ent_domain_families = {s for s in entity_scope if s.endswith("_") and not s.startswith("ENT_")}

        allowed_ids: Set[str] = set()
        type_counts = {"METRIC": 0, "DIM": 0, "ENT": 0, "VOCABULARY": 0, "OTHER": 0}

        for term_id, term_def in self.metadata_map.items():
            if not isinstance(term_id, str):
                continue
            if not isinstance(term_def, dict):
                continue

            ttype = _term_type(term_id)
            
            # VOCABULARY 类型（通用词汇映射）不对应真实业务数据表/字段，不应纳入 RBAC 数据权限控制
            # 因此跳过 domain 检查，直接允许
            if ttype == "VOCABULARY":
                allowed_ids.add(term_id)
                type_counts["VOCABULARY"] = type_counts.get("VOCABULARY", 0) + 1
                continue
            
            term_domain = term_def.get("domain_id")
            if not _domain_allowed(term_domain):
                continue

            allowed = False

            if ttype == "METRIC":
                if term_id in metric_explicit_ids:
                    allowed = True
                elif metric_has_all:
                    allowed = True
                else:
                    # domain 族规则：<DOMAIN>_ALL / <DOMAIN>_BASE
                    for rule in metric_domain_rules:
                        parts = rule.split("_", 1)
                        if len(parts) != 2:
                            continue
                        rule_domain, rule_tail = parts[0], parts[1]
                        if not term_domain or term_domain != rule_domain:
                            continue
                        if rule_tail == "ALL":
                            allowed = True
                            break
                        if rule_tail == "BASE":
                            category = term_def.get("category")
                            if category is None:
                                logger.warning(
                                    "Metric category missing while applying *_BASE rule; allowing to avoid false-deny",
                                    extra={"term_id": term_id, "role_id": role_id, "policy_id": policy_id},
                                )
                                allowed = True
                                break
                            if str(category).upper() in {"CORE", "BASE"}:
                                allowed = True
                                break

            elif ttype == "DIM":
                if term_id in dim_explicit_ids:
                    allowed = True
                elif dim_has_all:
                    allowed = True
                else:
                    for family in dim_domain_families:
                        family_domain = family[:-1]  # 去掉尾部 "_"
                        if term_domain == family_domain:
                            allowed = True
                            break

            elif ttype == "ENT":
                if term_id in ent_explicit_ids:
                    allowed = True
                elif ent_has_all:
                    allowed = True
                else:
                    for family in ent_domain_families:
                        family_domain = family[:-1]
                        if term_domain == family_domain:
                            allowed = True
                            break

            else:
                # OTHER 类型不在白名单前缀内，默认拒绝（稳定 fail-closed）
                allowed = False

            if allowed:
                allowed_ids.add(term_id)
                type_counts[ttype] = type_counts.get(ttype, 0) + 1

        logger.info(
            "RBAC allowlist computed",
            extra={
                "role_id": role_id,
                "policy_id": policy_id,
                "allowed_total": len(allowed_ids),
                "allowed_metric": type_counts.get("METRIC", 0),
                "allowed_dim": type_counts.get("DIM", 0),
                "allowed_ent": type_counts.get("ENT", 0),
                "allowed_vocab": type_counts.get("VOCABULARY", 0),
                "allowed_other": type_counts.get("OTHER", 0),
            },
        )

        self._allowed_ids_cache[role_id] = allowed_ids
        return allowed_ids

    # ============================================================
    # 时间窗口解析（语义配置驱动，禁止硬编码）
    # ============================================================
    def resolve_time_window(
        self,
        time_window_id: str,
        time_field_id: Optional[str] = None,
    ) -> Tuple["TimeRange", str]:
        """
        解析 time_window_id 为结构化的 TimeRange，并返回可读描述 time_desc。

        注意：
        - 仅从语义 YAML 配置（global_config.time_windows）解析，禁止任何硬编码默认值。
        - 解析失败必须抛 SemanticConfigurationError（code=CONFIGURATION_ERROR）。
        - time_field_id 目前用于上层做口径冲突检测与日志/提示拼装；TimeRange 本身不携带该字段。
        """
        # 延迟导入避免循环依赖（schemas.plan -> core.*）
        from schemas.plan import TimeRange, TimeRangeType

        if not time_window_id or not isinstance(time_window_id, str):
            raise SemanticConfigurationError(
                "Invalid time_window_id (empty or non-string)",
                details={"time_window_id": time_window_id, "time_field_id": time_field_id},
            )

        time_windows = []
        if isinstance(self.global_config, dict):
            time_windows = self.global_config.get("time_windows", []) or []

        if not isinstance(time_windows, list):
            raise SemanticConfigurationError(
                "global_config.time_windows must be a list",
                details={"time_windows_type": type(time_windows).__name__},
            )

        tw_def: Optional[Dict[str, Any]] = None
        for tw in time_windows:
            if isinstance(tw, dict) and tw.get("id") == time_window_id:
                tw_def = tw
                break

        if not tw_def:
            raise SemanticConfigurationError(
                f"time_window_id not found in global_config.time_windows: {time_window_id}",
                details={
                    "time_window_id": time_window_id,
                    "time_field_id": time_field_id,
                    "lookup_path": "global_config.time_windows[*].id",
                },
            )

        time_desc = tw_def.get("name") or time_window_id
        template = tw_def.get("template") if isinstance(tw_def.get("template"), dict) else None
        if not template:
            raise SemanticConfigurationError(
                f"time_window template missing or invalid for id={time_window_id}",
                details={
                    "time_window_id": time_window_id,
                    "lookup_path": "global_config.time_windows[*].template",
                },
            )

        tw_type = template.get("type")
        if tw_type == "LAST_N":
            value = template.get("value")
            unit = template.get("unit")
            if not isinstance(value, int) or value <= 0:
                raise SemanticConfigurationError(
                    f"time_window template.value must be positive int for id={time_window_id}",
                    details={
                        "time_window_id": time_window_id,
                        "template": template,
                    },
                )
            if not unit or not isinstance(unit, str):
                raise SemanticConfigurationError(
                    f"time_window template.unit must be string for id={time_window_id}",
                    details={
                        "time_window_id": time_window_id,
                        "template": template,
                    },
                )
            return TimeRange(type=TimeRangeType.LAST_N, value=value, unit=unit), time_desc

        if tw_type == "ABSOLUTE":
            start = template.get("start")
            end = template.get("end")
            # 允许只给 end（例如 CURRENT_DATE），但至少要有一个边界
            if start is None and end is None:
                raise SemanticConfigurationError(
                    f"time_window template.start/end both missing for id={time_window_id}",
                    details={
                        "time_window_id": time_window_id,
                        "template": template,
                    },
                )
            if start is not None and not isinstance(start, str):
                raise SemanticConfigurationError(
                    f"time_window template.start must be string for id={time_window_id}",
                    details={
                        "time_window_id": time_window_id,
                        "template": template,
                    },
                )
            if end is not None and not isinstance(end, str):
                raise SemanticConfigurationError(
                    f"time_window template.end must be string for id={time_window_id}",
                    details={
                        "time_window_id": time_window_id,
                        "template": template,
                    },
                )
            return TimeRange(type=TimeRangeType.ABSOLUTE, start=start, end=end), time_desc

        raise SemanticConfigurationError(
            f"Unsupported time_window template.type={tw_type} for id={time_window_id}",
            details={
                "time_window_id": time_window_id,
                "template": template,
            },
        )
    
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
    # 检索方法（按设计文档 3.2.3 的三步流程）
    # ============================================================
    
    def search_by_keyword(
        self,
        query: str,
        allowed_ids: Optional[Set[str]] = None
    ) -> Set[str]:
        """
        步骤一：关键词匹配搜索
        
        在 keyword_index 中搜索包含查询文本的术语。
        使用精确匹配（子串匹配）。
        
        Args:
            query: 查询文本
            allowed_ids: 允许的 ID 集合（用于权限过滤），如果为 None 则不过滤
        
        Returns:
            Set[str]: 匹配的术语 ID 集合
        """
        query_lower = query.lower()
        matches: Set[str] = set()
        
        # 遍历关键词索引
        for keyword, term_ids in self.keyword_index.items():
            if keyword.lower() in query_lower:
                # 添加匹配的术语 ID
                for term_id in term_ids:
                    # 权限过滤
                    if allowed_ids is None or term_id in allowed_ids:
                        matches.add(term_id)
        
        logger.debug(
            f"Keyword search completed: query='{query}', matches={len(matches)}"
        )
        
        return matches
    
    async def search_by_vector(
        self,
        query: str,
        allowed_ids: Optional[List[str]] = None,
        top_k: int = 20,
        similarity_threshold: float = 0.0
    ) -> List[Tuple[str, float]]:
        """
        步骤二：向量相似度搜索
        
        使用语义嵌入向量进行相似度搜索。
        
        Args:
            query: 查询文本
            allowed_ids: 允许的 ID 列表（用于权限过滤），如果为 None 则不过滤
            top_k: 返回的 top-k 结果数量
            similarity_threshold: 相似度阈值，低于此值的结果将被过滤
        
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
            # 注意：AsyncQdrantClient 使用 query_points 方法而不是 search 方法
            # query_points 返回 QueryResponse 对象，结果在 points 属性中
            search_response = await self.qdrant_client.query_points(
                collection_name=QDRANT_COLLECTION_NAME,
                query=query_embedding,
                query_filter=qdrant_filter,
                limit=top_k
            )
            
            # 提取结果并应用相似度阈值（QueryResponse.points 是 ScoredPoint 列表）
            results = [
                (point.payload.get("id"), point.score)
                for point in search_response.points
                if point.payload 
                and "id" in point.payload
                and point.score >= similarity_threshold
            ]
            
            logger.debug(
                f"Vector search completed: query='{query}', "
                f"results={len(results)} (threshold={similarity_threshold})"
            )
            
            return results
        
        except Exception as e:
            logger.error(f"Error in vector search: {e}")
            raise
    
    def merge_search_results(
        self,
        keyword_matches: Set[str],
        vector_results: List[Tuple[str, float]],
        max_recall: Optional[int] = None
    ) -> List[str]:
        """
        步骤三：合并和排序搜索结果
        
        合并关键词匹配和向量搜索的结果，优先保留关键词匹配的结果。
        
        Args:
            keyword_matches: 关键词匹配的术语 ID 集合
            vector_results: 向量搜索的结果列表 [(term_id, score), ...]
            max_recall: 最大召回数量，如果为 None 则不限制
        
        Returns:
            List[str]: 合并后的术语 ID 列表，按优先级排序（关键词匹配优先）
        """
        # 提取向量搜索的术语 ID（排除已在关键词匹配中的）
        vector_matches = {
            term_id for term_id, _ in vector_results
            if term_id not in keyword_matches
        }
        
        # 合并结果：关键词匹配优先，然后是向量匹配
        final_terms = list(keyword_matches) + list(vector_matches)
        
        # 应用最大召回限制
        if max_recall is not None and max_recall > 0:
            final_terms = final_terms[:max_recall]
        
        logger.debug(
            f"Search results merged: total={len(final_terms)}, "
            f"keyword={len(keyword_matches)}, vector={len(vector_matches)}"
        )
        
        return final_terms
    
    # ============================================================
    # 向后兼容的别名方法
    # ============================================================
    
    async def search_similar_terms(
        self,
        query: str,
        allowed_ids: Optional[List[str]] = None,
        top_k: int = 20
    ) -> List[Tuple[str, float]]:
        """
        搜索相似的术语（向量搜索）- 向后兼容方法
        
        此方法调用 search_by_vector，保持向后兼容性。
        
        Args:
            query: 查询文本
            allowed_ids: 允许的 ID 列表（用于权限过滤），如果为 None 则不过滤
            top_k: 返回的 top-k 结果数量
        
        Returns:
            List[Tuple[str, float]]: [(term_id, score), ...] 列表，按相似度降序排列
        """
        return await self.search_by_vector(
            query=query,
            allowed_ids=allowed_ids,
            top_k=top_k,
            similarity_threshold=0.0
        )


# ============================================================
# 全局单例访问函数
# ============================================================
_semantic_registry: Optional[SemanticRegistry] = None


async def get_semantic_registry() -> SemanticRegistry:
    """
    获取 SemanticRegistry 单例实例
    
    Returns:
        SemanticRegistry: SemanticRegistry 实例
    """
    return await SemanticRegistry.get_instance()


"""
AI Client Module

统一的多提供商 AI 客户端，支持通过配置路由到不同的 AI 提供商。
"""
import os
from typing import Any, Dict, List, Optional, Tuple

from utils.log_manager import get_logger
from .providers.base import BaseAIProvider
from .providers.openai_provider import OpenAIProvider
from .providers.jina_provider import JinaProvider

logger = get_logger(__name__)

# Provider 类型映射
PROVIDER_TYPE_MAP = {
    "openai": OpenAIProvider,
    "jina": JinaProvider,
}


class AIClient:
    """
    统一 AI 客户端
    
    管理多个 AI 提供商实例，根据 usage_key 路由到相应的提供商和模型。
    提供语义化的方法接口，隐藏提供商细节。
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化 AI 客户端
        
        Args:
            config: 配置字典，包含：
                - default_provider: 默认提供商名称
                - providers: 提供商配置字典
                - model_mapping: usage_key -> (provider, model) 映射
        """
        self.config = config or self._default_config()
        self._providers: Dict[str, BaseAIProvider] = {}
        
        # 初始化所有提供商
        self._init_providers()
        
        logger.info(
            "AIClient initialized",
            extra={
                "providers": list(self._providers.keys()),
                "model_mappings": list(self.config.get("model_mapping", {}).keys())
            }
        )
    
    def _default_config(self) -> Dict[str, Any]:
        """生成默认配置（从环境变量读取）"""
        return {
            "default_provider": "openai",
            "providers": {
                "openai": {
                    "api_key": os.getenv("OPENAI_API_KEY"),
                    "base_url": os.getenv("OPENAI_BASE_URL"),
                },
                "jina": {
                    "api_key": os.getenv("JINA_API_KEY"),
                    "base_url": os.getenv("JINA_BASE_URL"),
                },
            },
            "model_mapping": {
                "query_decomposition": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "plan_generation": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "answer_generation": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "embedding": {
                    "provider": "jina",
                    "model": "jina-embeddings-v3"
                },
            },
        }
    
    def _init_providers(self) -> None:
        """初始化所有提供商实例（支持动态 provider 配置）"""
        providers_config = self.config.get("providers", {})
        
        # 动态遍历所有 provider 配置
        for provider_name, provider_config in providers_config.items():
            api_key = provider_config.get("api_key")
            if not api_key:
                logger.warning(
                    f"API key not set for provider '{provider_name}', "
                    f"provider will not be available"
                )
                continue
            
            # 确定 Provider 类
            # 优先使用显式的 type 字段
            provider_type = provider_config.get("type")
            
            # 如果没有 type 字段，根据 provider 名称推断（向后兼容）
            if not provider_type:
                if provider_name == "openai" or provider_name == "deepseek":
                    provider_type = "openai"
                elif provider_name == "jina":
                    provider_type = "jina"
                else:
                    logger.warning(
                        f"Unknown provider type for '{provider_name}'. "
                        f"Please specify 'type' field in config. Skipping."
                    )
                    continue
            
            # 获取对应的 Provider 类
            provider_class = PROVIDER_TYPE_MAP.get(provider_type)
            if not provider_class:
                logger.warning(
                    f"Unknown provider type '{provider_type}' for '{provider_name}'. "
                    f"Available types: {list(PROVIDER_TYPE_MAP.keys())}. Skipping."
                )
                continue
            
            # 初始化 Provider 实例
            try:
                self._providers[provider_name] = provider_class(
                    api_key=api_key,
                    base_url=provider_config.get("base_url")
                )
                logger.debug(
                    f"Initialized provider '{provider_name}' with type '{provider_type}'",
                    extra={"base_url": provider_config.get("base_url") or "default"}
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialize provider '{provider_name}': {e}",
                    extra={"provider_type": provider_type}
                )
                continue
    
    def _resolve_model(self, usage_key: str) -> Tuple[BaseAIProvider, str]:
        """
        根据 usage_key 解析提供商和模型
        
        Args:
            usage_key: 使用场景标识（如 "plan_generation", "answer_generation"）
        
        Returns:
            Tuple[BaseAIProvider, str]: (提供商实例, 模型名称)
        
        Raises:
            ValueError: 如果 usage_key 不存在或提供商未初始化
        """
        model_mapping = self.config.get("model_mapping", {})
        
        if usage_key not in model_mapping:
            raise ValueError(
                f"Unknown usage_key: {usage_key}. "
                f"Available keys: {list(model_mapping.keys())}"
            )
        
        mapping = model_mapping[usage_key]
        provider_name = mapping.get("provider") or self.config.get("default_provider")
        model_name = mapping.get("model")
        
        if not provider_name:
            raise ValueError(f"No provider specified for usage_key: {usage_key}")
        
        if provider_name not in self._providers:
            raise ValueError(
                f"Provider '{provider_name}' not initialized. "
                f"Available providers: {list(self._providers.keys())}"
            )
        
        if not model_name:
            raise ValueError(f"No model specified for usage_key: {usage_key}")
        
        return self._providers[provider_name], model_name
    
    # ============================================================
    # 语义化方法接口
    # ============================================================
    
    async def generate_plan(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        生成查询计划（返回 JSON）
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            **kwargs: 其他参数
        
        Returns:
            Dict[str, Any]: 解析后的 JSON 对象
        """
        provider, model = self._resolve_model("plan_generation")
        return await provider.chat_json(
            messages=messages,
            model=model,
            temperature=temperature,
            **kwargs
        )
    
    async def generate_decomposition(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        生成查询分解（返回 JSON）
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            **kwargs: 其他参数
        
        Returns:
            Dict[str, Any]: 解析后的 JSON 对象
        """
        provider, model = self._resolve_model("query_decomposition")
        return await provider.chat_json(
            messages=messages,
            model=model,
            temperature=temperature,
            **kwargs
        )
    
    async def generate_answer(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        **kwargs: Any
    ) -> str:
        """
        生成自然语言答案（返回文本）
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            **kwargs: 其他参数
        
        Returns:
            str: 生成的文本内容
        """
        provider, model = self._resolve_model("answer_generation")
        return await provider.chat(
            messages=messages,
            model=model,
            temperature=temperature,
            **kwargs
        )
    
    async def get_embeddings(
        self,
        texts: List[str],
        **kwargs: Any
    ) -> List[List[float]]:
        """
        获取文本嵌入向量
        
        Args:
            texts: 文本列表
            **kwargs: 其他参数
        
        Returns:
            List[List[float]]: 嵌入向量列表
        """
        provider, model = self._resolve_model("embedding")
        return await provider.embed(
            texts=texts,
            model=model,
            **kwargs
        )
    
    # ============================================================
    # 通用方法接口
    # ============================================================
    
    async def call_model(
        self,
        usage_key: str,
        *,
        messages: Optional[List[Dict[str, str]]] = None,
        texts: Optional[List[str]] = None,
        **kwargs: Any
    ) -> Any:
        """
        通用模型调用方法
        
        Args:
            usage_key: 使用场景标识
            messages: 消息列表（用于聊天）
            texts: 文本列表（用于嵌入）
            **kwargs: 其他参数
        
        Returns:
            Any: 根据 usage_key 返回相应类型的结果
        """
        provider, model = self._resolve_model(usage_key)
        
        # 根据 usage_key 判断调用哪个方法
        if usage_key == "embedding":
            if not texts:
                raise ValueError("texts parameter is required for embedding usage_key")
            return await provider.embed(texts=texts, model=model, **kwargs)
        else:
            if not messages:
                raise ValueError("messages parameter is required for chat usage_key")
            # 检查是否是 JSON 模式（通过 usage_key 判断）
            if usage_key in ["plan_generation", "query_decomposition"]:
                return await provider.chat_json(
                    messages=messages,
                    model=model,
                    **kwargs
                )
            else:
                return await provider.chat(
                    messages=messages,
                    model=model,
                    **kwargs
                )
    
    @classmethod
    def init_from_settings(cls, settings: Any) -> "AIClient":
        """
        从设置对象初始化 AI 客户端
        
        Args:
            settings: 设置对象（如 PipelineConfig）
        
        Returns:
            AIClient: 初始化的 AI 客户端实例
        """
        # 辅助函数：从 settings 或环境变量读取配置值
        def get_config_value(key: str, default: Optional[str] = None) -> Optional[str]:
            """从 settings 对象或环境变量读取配置值"""
            value = getattr(settings, key, None)
            if value:
                return value
            return os.getenv(key, default)
        
        # 构建 providers 配置
        providers_config = {
            "openai": {
                "api_key": get_config_value("OPENAI_API_KEY"),
                "base_url": get_config_value("OPENAI_BASE_URL"),
                "type": "openai",
            },
            "jina": {
                "api_key": get_config_value("JINA_API_KEY"),
                "base_url": get_config_value("JINA_BASE_URL"),
                "type": "jina",
            },
        }
        
        # 添加 DeepSeek 配置（如果提供了 API Key）
        deepseek_api_key = get_config_value("DEEPSEEK_API_KEY")
        if deepseek_api_key:
            providers_config["deepseek"] = {
                "api_key": deepseek_api_key,
                "base_url": get_config_value("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
                "type": "openai",  # DeepSeek 使用 OpenAI 兼容的 API
            }
        
        config = {
            "default_provider": "openai",
            "providers": providers_config,
            "model_mapping": {
                "query_decomposition": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "plan_generation": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "answer_generation": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "embedding": {
                    "provider": "jina",
                    "model": "jina-embeddings-v3"
                },
            },
        }
        
        return cls(config)


# ============================================================
# 全局单例实例
# ============================================================
_ai_client: Optional[AIClient] = None


def get_ai_client() -> AIClient:
    """
    获取全局 AI 客户端实例（单例模式）
    
    Returns:
        AIClient: 全局 AI 客户端实例
    """
    global _ai_client
    if _ai_client is None:
        _ai_client = AIClient()
    return _ai_client

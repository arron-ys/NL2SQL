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


class AIProviderInitError(Exception):
    """AI Provider 初始化失败（用于快速定位/对外映射 503）。"""

    def __init__(self, provider_name: str, reason: str):
        super().__init__(f"Failed to initialize provider '{provider_name}': {reason}")
        self.provider_name = provider_name
        self.reason = reason


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
        
        # 记录初始化后的 provider 列表（用于排查初始化问题）
        instance_id = id(self)
        pid = os.getpid()
        providers = list(self._providers.keys())
        logger.debug(
            f"AIClient 初始化详情 | PID={pid} | ID={instance_id} | Providers={providers}"
        )
        
        # 合并为一条简洁的日志
        provider_names = ', '.join(providers) if providers else '无'
        logger.info(
            f"AI 客户端已初始化 | Providers: {provider_names}",
            extra={
                "providers": providers,
                "model_mappings": list(self.config.get("model_mapping", {}).keys())
            }
        )
    
    async def close(self):
        """
        关闭所有 provider 的连接（Option B：资源管理）
        
        在应用关闭时调用，确保所有连接正确清理。
        """
        for provider_name, provider in self._providers.items():
            try:
                if hasattr(provider, 'close'):
                    await provider.close()
                    logger.debug(f"Closed provider: {provider_name}")
            except Exception as e:
                logger.error(f"Error closing provider {provider_name}: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        获取所有 provider 的统计指标（中期：监控&告警）
        
        Returns:
            Dict[str, Any]: 所有 provider 的指标快照
        """
        metrics = {}
        for provider_name, provider in self._providers.items():
            if hasattr(provider, 'metrics'):
                metrics[provider_name] = provider.metrics.to_dict()
        return metrics
    
    async def healthcheck_all(self) -> Dict[str, bool]:
        """
        对所有 provider 执行健康检查（长期：连接健康检查 + 自愈）
        
        Returns:
            Dict[str, bool]: provider_name -> 健康状态
        """
        results = {}
        for provider_name, provider in self._providers.items():
            if hasattr(provider, 'healthcheck'):
                try:
                    results[provider_name] = await provider.healthcheck()
                except Exception as e:
                    logger.debug(f"{provider_name} 连接检查失败: {e}")
                    results[provider_name] = False
            else:
                results[provider_name] = True  # 没有 healthcheck 方法则认为健康
        return results
    
    def _default_config(self) -> Dict[str, Any]:
        """生成默认配置（从环境变量读取）"""
        # 读取 API keys
        openai_key = os.getenv("OPENAI_API_KEY")
        jina_key = os.getenv("JINA_API_KEY")
        deepseek_key = os.getenv("DEEPSEEK_API_KEY")
        qwen_key = os.getenv("QWEN_API_KEY")
        
        # 读取模型配置（支持从环境变量配置，提供默认值）
        # OpenAI 模型配置
        openai_model_query_decomposition = os.getenv("OPENAI_MODEL_QUERY_DECOMPOSITION", "gpt-4o-mini")
        openai_model_plan_generation = os.getenv("OPENAI_MODEL_PLAN_GENERATION", "gpt-4o-mini")
        openai_model_answer_generation = os.getenv("OPENAI_MODEL_ANSWER_GENERATION", "gpt-4o-mini")
        
        # DeepSeek 模型配置（默认值：chat 用于对话，reasoner 用于推理）
        deepseek_model_query_decomposition = os.getenv("DEEPSEEK_MODEL_QUERY_DECOMPOSITION", "deepseek-chat")
        deepseek_model_plan_generation = os.getenv("DEEPSEEK_MODEL_PLAN_GENERATION", "deepseek-reasoner")
        deepseek_model_answer_generation = os.getenv("DEEPSEEK_MODEL_ANSWER_GENERATION", "deepseek-chat")
        
        # Qwen 模型配置（默认值：turbo 快速，max 高质量，plus 平衡）
        qwen_model_query_decomposition = os.getenv("QWEN_MODEL_QUERY_DECOMPOSITION", "qwen-turbo")
        qwen_model_plan_generation = os.getenv("QWEN_MODEL_PLAN_GENERATION", "qwen-max")
        qwen_model_answer_generation = os.getenv("QWEN_MODEL_ANSWER_GENERATION", "qwen-plus")
        
        # Jina 模型配置
        jina_model_embedding = os.getenv("JINA_MODEL_EMBEDDING", "jina-embeddings-v3")
        
        # 构建 providers 配置
        providers_config = {
            "openai": {
                "api_key": openai_key,
                "base_url": os.getenv("OPENAI_BASE_URL"),
                "type": "openai",
            },
            "jina": {
                "api_key": jina_key,
                "base_url": os.getenv("JINA_BASE_URL"),
                "type": "jina",
            },
        }
        
        # 添加 DeepSeek 配置（如果提供了 API Key）
        if deepseek_key:
            providers_config["deepseek"] = {
                "api_key": deepseek_key,
                "base_url": os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
                "type": "openai",  # DeepSeek 使用 OpenAI 兼容的 API
            }
        
        # 添加 Qwen 配置（如果提供了 API Key）
        if qwen_key:
            providers_config["qwen"] = {
                "api_key": qwen_key,
                "base_url": os.getenv(
                    "QWEN_BASE_URL",
                    "https://dashscope.aliyuncs.com/compatible-mode/v1"
                ),
                "type": "openai",  # Qwen 使用 OpenAI 兼容的 API
            }
        
        # 确定 model_mapping 中的 provider
        # 优先级：1. DEFAULT_LLM_PROVIDER 环境变量（明确指定）
        #         2. 自动选择（DeepSeek > Qwen > OpenAI）
        default_llm_provider = os.getenv("DEFAULT_LLM_PROVIDER", "").lower()
        
        # 验证指定的 provider 是否配置了 API Key
        if default_llm_provider:
            if default_llm_provider == "deepseek" and not deepseek_key:
                logger.warning(
                    "DEFAULT_LLM_PROVIDER is set to 'deepseek' but DEEPSEEK_API_KEY is not configured. "
                    "Falling back to auto-selection."
                )
                default_llm_provider = ""
            elif default_llm_provider == "qwen" and not qwen_key:
                logger.warning(
                    "DEFAULT_LLM_PROVIDER is set to 'qwen' but QWEN_API_KEY is not configured. "
                    "Falling back to auto-selection."
                )
                default_llm_provider = ""
            elif default_llm_provider == "openai" and not openai_key:
                logger.warning(
                    "DEFAULT_LLM_PROVIDER is set to 'openai' but OPENAI_API_KEY is not configured. "
                    "Falling back to auto-selection."
                )
                default_llm_provider = ""
            elif default_llm_provider not in ["openai", "deepseek", "qwen"]:
                logger.warning(
                    f"DEFAULT_LLM_PROVIDER is set to '{default_llm_provider}' which is not supported. "
                    "Supported values: openai, deepseek, qwen. Falling back to auto-selection."
                )
                default_llm_provider = ""
        
        # 如果没有明确指定或指定无效，使用自动选择逻辑
        if not default_llm_provider:
            if deepseek_key:
                default_llm_provider = "deepseek"
            elif qwen_key:
                default_llm_provider = "qwen"
            else:
                default_llm_provider = "openai"
        
        # 根据 provider 选择对应的模型
        if default_llm_provider == "deepseek":
            query_model = deepseek_model_query_decomposition
            plan_model = deepseek_model_plan_generation
            answer_model = deepseek_model_answer_generation
        elif default_llm_provider == "qwen":
            query_model = qwen_model_query_decomposition
            plan_model = qwen_model_plan_generation
            answer_model = qwen_model_answer_generation
        else:
            query_model = openai_model_query_decomposition
            plan_model = openai_model_plan_generation
            answer_model = openai_model_answer_generation
        
        # 记录 API key 读取情况（用于调试）
        logger.info(
            "LLM Provider configuration",
            extra={
                "openai_key_exists": bool(openai_key),
                "deepseek_key_exists": bool(deepseek_key),
                "qwen_key_exists": bool(qwen_key),
                "jina_key_exists": bool(jina_key),
                "default_llm_provider": default_llm_provider,
                "query_model": query_model,
                "plan_model": plan_model,
                "answer_model": answer_model,
                "jina_model_embedding": jina_model_embedding
            }
        )
        
        return {
            "default_provider": default_llm_provider,
            "providers": providers_config,
            "model_mapping": {
                "query_decomposition": {
                    "provider": default_llm_provider,
                    "model": query_model
                },
                "plan_generation": {
                    "provider": default_llm_provider,
                    "model": plan_model
                },
                "answer_generation": {
                    "provider": default_llm_provider,
                    "model": answer_model
                },
                "embedding": {
                    "provider": "jina",
                    "model": jina_model_embedding
                },
            },
        }
    
    def _init_providers(self) -> None:
        """初始化所有提供商实例（支持动态 provider 配置）"""
        providers_config = self.config.get("providers", {})

        # 计算“必须可用”的 provider：被 model_mapping 引用的 provider + default_provider
        required_providers = set()
        try:
            model_mapping = self.config.get("model_mapping", {}) or {}
            for _, mapping in model_mapping.items():
                if isinstance(mapping, dict) and mapping.get("provider"):
                    required_providers.add(str(mapping["provider"]))
            default_provider = self.config.get("default_provider")
            if default_provider:
                required_providers.add(str(default_provider))
        except Exception:
            # 计算 required_providers 失败不应阻断初始化；后续 _resolve_model 会兜底报错
            required_providers = set()
        
        # 动态遍历所有 provider 配置
        for provider_name, provider_config in providers_config.items():
            api_key = provider_config.get("api_key")
            
            # 检查 API key 是否存在（包括空字符串的情况）
            if not api_key or (isinstance(api_key, str) and not api_key.strip()):
                logger.warning(
                    f"API key not set for provider '{provider_name}', "
                    f"provider will not be available. "
                    f"Please set {provider_name.upper()}_API_KEY in .env file. "
                    f"Current value: {repr(api_key)}"
                )
                continue
            
            # 记录 API key 已设置（但不记录实际值，避免泄露）
            logger.debug(
                f"API key found for provider '{provider_name}'",
                extra={"key_length": len(api_key) if api_key else 0}
            )
            
            # 确定 Provider 类
            # 优先使用显式的 type 字段
            provider_type = provider_config.get("type")
            
            # 如果没有 type 字段，根据 provider 名称推断（向后兼容）
            # 注意：对于 OpenAI 兼容的 provider（如 deepseek、qwen），必须显式指定 type="openai"
            if not provider_type:
                if provider_name == "openai":
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
                # 构建初始化参数
                init_kwargs = {
                    "api_key": api_key,
                    "base_url": provider_config.get("base_url")
                }
                
                # 对于 OpenAI 类型的 provider，添加超时和代理配置
                if provider_type == "openai":
                    # 超时配置：优先使用通用 LLM_TIMEOUT，否则使用 provider 特定配置，最后使用默认值
                    # 注意：超时配置会在 OpenAIProvider 内部处理，这里不需要显式传递
                    # 代理配置：从环境变量或配置中读取
                    init_kwargs["proxy"] = provider_config.get("proxy")
                    # 传递 provider_name，用于 proxy env 分流（OPENAI_PROXY/DEEPSEEK_PROXY/QWEN_PROXY）
                    init_kwargs["provider_name"] = provider_name
                    # timeout 参数会在 OpenAIProvider.__init__ 中从环境变量读取，不需要在这里传递
                
                logger.debug(
                    f"Initializing provider '{provider_name}' with type '{provider_type}'",
                    extra={
                        "has_api_key": bool(api_key),
                        "api_key_length": len(api_key) if api_key else 0,
                        "has_proxy": bool(init_kwargs.get("proxy")),
                        "timeout": init_kwargs.get("timeout")
                    }
                )
                
                self._providers[provider_name] = provider_class(**init_kwargs)
                logger.debug(
                    f"Successfully initialized provider '{provider_name}' with type '{provider_type}'",
                    extra={"base_url": provider_config.get("base_url") or "default"}
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialize provider '{provider_name}': {e}",
                    extra={
                        "provider_type": provider_type,
                        "error_type": type(e).__name__,
                        "has_api_key": bool(api_key),
                        "api_key_length": len(api_key) if api_key else 0
                    },
                    exc_info=True
                )
                # 如果该 provider 是关键路径（会被路由使用），则直接 fail-fast 抛出明确异常
                if provider_name in required_providers:
                    raise AIProviderInitError(provider_name=provider_name, reason=str(e)) from e
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
            # 记录缺失 provider 时的详细信息（用于排查问题）
            instance_id = id(self)
            pid = os.getpid()
            logger.error(
                f"Missing Provider: '{provider_name}' | "
                f"Available Providers: {list(self._providers.keys())} | "
                f"Usage Key: '{usage_key}' | "
                f"PID={pid} | ID={instance_id}",
                extra={
                    "provider_name": provider_name,
                    "available_providers": list(self._providers.keys()),
                    "usage_key": usage_key,
                    "instance_id": instance_id,
                    "pid": pid
                }
            )
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
        
        # 添加 Qwen 配置（如果提供了 API Key）
        qwen_api_key = get_config_value("QWEN_API_KEY")
        if qwen_api_key:
            providers_config["qwen"] = {
                "api_key": qwen_api_key,
                "base_url": get_config_value(
                    "QWEN_BASE_URL", 
                    "https://dashscope.aliyuncs.com/compatible-mode/v1"
                ),
                "type": "openai",  # Qwen 使用 OpenAI 兼容的 API
            }
        
        # 读取模型配置（支持从环境变量配置，提供默认值）
        # OpenAI 模型配置
        openai_model_query_decomposition = get_config_value("OPENAI_MODEL_QUERY_DECOMPOSITION", "gpt-4o-mini")
        openai_model_plan_generation = get_config_value("OPENAI_MODEL_PLAN_GENERATION", "gpt-4o-mini")
        openai_model_answer_generation = get_config_value("OPENAI_MODEL_ANSWER_GENERATION", "gpt-4o-mini")
        
        # DeepSeek 模型配置
        deepseek_model_query_decomposition = get_config_value("DEEPSEEK_MODEL_QUERY_DECOMPOSITION", "deepseek-chat")
        deepseek_model_plan_generation = get_config_value("DEEPSEEK_MODEL_PLAN_GENERATION", "deepseek-reasoner")
        deepseek_model_answer_generation = get_config_value("DEEPSEEK_MODEL_ANSWER_GENERATION", "deepseek-chat")
        
        # Qwen 模型配置
        qwen_model_query_decomposition = get_config_value("QWEN_MODEL_QUERY_DECOMPOSITION", "qwen-turbo")
        qwen_model_plan_generation = get_config_value("QWEN_MODEL_PLAN_GENERATION", "qwen-max")
        qwen_model_answer_generation = get_config_value("QWEN_MODEL_ANSWER_GENERATION", "qwen-plus")
        
        # Jina 模型配置
        jina_model_embedding = get_config_value("JINA_MODEL_EMBEDDING", "jina-embeddings-v3")
        
        # 确定 model_mapping 中的 provider
        # 优先级：1. DEFAULT_LLM_PROVIDER 环境变量（明确指定）
        #         2. 自动选择（DeepSeek > Qwen > OpenAI）
        default_llm_provider = get_config_value("DEFAULT_LLM_PROVIDER", "").lower()
        
        # 验证指定的 provider 是否配置了 API Key
        if default_llm_provider:
            if default_llm_provider == "deepseek" and not deepseek_api_key:
                logger.warning(
                    "DEFAULT_LLM_PROVIDER is set to 'deepseek' but DEEPSEEK_API_KEY is not configured. "
                    "Falling back to auto-selection."
                )
                default_llm_provider = ""
            elif default_llm_provider == "qwen" and not qwen_api_key:
                logger.warning(
                    "DEFAULT_LLM_PROVIDER is set to 'qwen' but QWEN_API_KEY is not configured. "
                    "Falling back to auto-selection."
                )
                default_llm_provider = ""
            elif default_llm_provider == "openai" and not get_config_value("OPENAI_API_KEY"):
                logger.warning(
                    "DEFAULT_LLM_PROVIDER is set to 'openai' but OPENAI_API_KEY is not configured. "
                    "Falling back to auto-selection."
                )
                default_llm_provider = ""
            elif default_llm_provider not in ["openai", "deepseek", "qwen"]:
                logger.warning(
                    f"DEFAULT_LLM_PROVIDER is set to '{default_llm_provider}' which is not supported. "
                    "Supported values: openai, deepseek, qwen. Falling back to auto-selection."
                )
                default_llm_provider = ""
        
        # 如果没有明确指定或指定无效，使用自动选择逻辑
        if not default_llm_provider:
            if deepseek_api_key:
                default_llm_provider = "deepseek"
            elif qwen_api_key:
                default_llm_provider = "qwen"
            else:
                default_llm_provider = "openai"
        
        # 根据 provider 选择对应的模型
        if default_llm_provider == "deepseek":
            query_model = deepseek_model_query_decomposition
            plan_model = deepseek_model_plan_generation
            answer_model = deepseek_model_answer_generation
        elif default_llm_provider == "qwen":
            query_model = qwen_model_query_decomposition
            plan_model = qwen_model_plan_generation
            answer_model = qwen_model_answer_generation
        else:
            query_model = openai_model_query_decomposition
            plan_model = openai_model_plan_generation
            answer_model = openai_model_answer_generation
        
        config = {
            "default_provider": default_llm_provider,
            "providers": providers_config,
            "model_mapping": {
                "query_decomposition": {
                    "provider": default_llm_provider,
                    "model": query_model
                },
                "plan_generation": {
                    "provider": default_llm_provider,
                    "model": plan_model
                },
                "answer_generation": {
                    "provider": default_llm_provider,
                    "model": answer_model
                },
                "embedding": {
                    "provider": "jina",
                    "model": jina_model_embedding
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
    
    增强功能：
    - 防御性环境变量加载：确保 .env 文件已加载
    - 全链路日志：记录初始化前后的状态，便于诊断问题
    
    Returns:
        AIClient: 全局 AI 客户端实例
    """
    global _ai_client
    if _ai_client is None:
        # 防御性加载：确保环境变量已就绪（无论调用时机如何）
        try:
            from dotenv import load_dotenv
            load_dotenv(override=False)  # 如果已加载则不覆盖，避免破坏已有配置
        except ImportError:
            logger.warning("python-dotenv not available, skipping defensive load_dotenv()")
        except Exception as e:
            logger.warning(f"Failed to load .env file defensively: {e}")
        
        # 记录初始化前的环境变量状态（用于诊断）
        jina_key = os.getenv("JINA_API_KEY")
        openai_key = os.getenv("OPENAI_API_KEY")
        deepseek_key = os.getenv("DEEPSEEK_API_KEY")
        qwen_key = os.getenv("QWEN_API_KEY")
        
        logger.info(
            "Creating new AIClient instance (defensive initialization)",
            extra={
                "jina_key_exists": bool(jina_key),
                "jina_key_length": len(jina_key) if jina_key else 0,
                "openai_key_exists": bool(openai_key),
                "openai_key_length": len(openai_key) if openai_key else 0,
                "deepseek_key_exists": bool(deepseek_key),
                "deepseek_key_length": len(deepseek_key) if deepseek_key else 0,
                "qwen_key_exists": bool(qwen_key),
                "qwen_key_length": len(qwen_key) if qwen_key else 0,
            }
        )
        
        # 创建实例
        _ai_client = AIClient()
        
        # 如果 jina_key 存在但 jina provider 未初始化，发出警告
        initialized_providers = list(_ai_client._providers.keys())
        if jina_key and "jina" not in initialized_providers:
            logger.error(
                "JINA_API_KEY 已配置但 Provider 未初始化！请检查初始化逻辑",
                extra={
                    "jina_key_exists": True,
                    "jina_key_length": len(jina_key),
                    "initialized_providers": initialized_providers,
                }
        )
    
    return _ai_client

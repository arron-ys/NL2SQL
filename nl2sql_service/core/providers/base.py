"""
Base AI Provider Module

定义所有 AI 提供商的抽象基类接口。
"""
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional


class BaseAIProvider(ABC):
    """
    AI 提供商抽象基类
    
    定义所有 AI 提供商必须实现的统一接口。
    包括聊天、嵌入、流式输出、工具调用、重排序等功能。
    """
    
    @abstractmethod
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> str:
        """
        执行聊天补全（返回文本）
        
        Args:
            messages: 消息列表，格式为 [{"role": "user", "content": "..."}, ...]
            model: 使用的模型名称
            temperature: 温度参数，控制输出的随机性（0-2）
            max_tokens: 最大生成 token 数
            **kwargs: 其他提供商特定的参数
        
        Returns:
            str: 模型返回的文本内容
        
        Raises:
            Exception: 当 API 调用失败时抛出异常
        """
        raise NotImplementedError
    
    @abstractmethod
    async def chat_json(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        执行聊天补全（返回 JSON 对象）
        
        Args:
            messages: 消息列表，格式为 [{"role": "user", "content": "..."}, ...]
            model: 使用的模型名称
            temperature: 温度参数，控制输出的随机性（0-2）
            max_tokens: 最大生成 token 数
            **kwargs: 其他提供商特定的参数
        
        Returns:
            Dict[str, Any]: 解析后的 JSON 对象
        
        Raises:
            Exception: 当 API 调用失败时抛出异常
        """
        raise NotImplementedError
    
    @abstractmethod
    async def embed(
        self,
        texts: List[str],
        model: str,
        **kwargs: Any
    ) -> List[List[float]]:
        """
        生成文本嵌入向量
        
        Args:
            texts: 要嵌入的文本列表
            model: 使用的嵌入模型名称
            **kwargs: 其他提供商特定的参数
        
        Returns:
            List[List[float]]: 嵌入向量列表，每个文本对应一个向量
        
        Raises:
            Exception: 当 API 调用失败时抛出异常
        """
        raise NotImplementedError
    
    @abstractmethod
    async def stream_chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> AsyncIterator[str]:
        """
        流式聊天补全（用于 WebSocket 推送）
        
        Args:
            messages: 消息列表，格式为 [{"role": "user", "content": "..."}, ...]
            model: 使用的模型名称
            temperature: 温度参数，控制输出的随机性（0-2）
            max_tokens: 最大生成 token 数
            **kwargs: 其他提供商特定的参数
        
        Yields:
            str: 流式返回的文本片段
        
        Raises:
            Exception: 当 API 调用失败时抛出异常
        """
        raise NotImplementedError
    
    @abstractmethod
    async def chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict[str, Any]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        带工具调用的聊天补全（用于 Agentic 工作流）
        
        Args:
            messages: 消息列表，格式为 [{"role": "user", "content": "..."}, ...]
            tools: 工具定义列表，格式为 [{"type": "function", "function": {...}}, ...]
            model: 使用的模型名称
            temperature: 温度参数，控制输出的随机性（0-2）
            max_tokens: 最大生成 token 数
            **kwargs: 其他提供商特定的参数
        
        Returns:
            Dict[str, Any]: 包含工具调用信息的响应对象
        
        Raises:
            Exception: 当 API 调用失败时抛出异常
        """
        raise NotImplementedError
    
    @abstractmethod
    async def rerank(
        self,
        query: str,
        documents: List[str],
        model: str,
        top_n: Optional[int] = None,
        **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        重排序文档（用于 RAG / 术语选择）
        
        Args:
            query: 查询文本
            documents: 文档列表
            model: 使用的重排序模型名称
            top_n: 返回的 top-n 结果数量，如果为 None 则返回所有结果
            **kwargs: 其他提供商特定的参数
        
        Returns:
            List[Dict[str, Any]]: 重排序后的结果列表，每个元素包含文档和分数
        
        Raises:
            Exception: 当 API 调用失败时抛出异常
        """
        raise NotImplementedError

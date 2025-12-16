"""
Live Test Helpers

提供 Live 测试所需的辅助函数，包括占位符 Key 检测等。
"""
import os
import re


def is_placeholder_key(value: str) -> bool:
    """
    判断 API Key 是否为占位符值（用于跳过 Live 测试）
    
    Args:
        value: API Key 字符串
        
    Returns:
        bool: 如果是占位符则返回 True
    """
    if not value or not isinstance(value, str):
        return True
    
    value_lower = value.lower()
    
    # 检查是否包含常见的占位符关键词
    placeholder_patterns = [
        r'fake',
        r'test',
        r'placeholder',
        r'example',
        r'dummy',
        r'sample',
        r'xxx',
        r'your.*key',
        r'your.*api',
    ]
    
    for pattern in placeholder_patterns:
        if re.search(pattern, value_lower):
            return True
    
    # 检查是否过短（可能是占位符）
    if len(value.strip()) < 10:
        return True
    
    return False


def get_openai_api_key() -> str:
    """获取 OpenAI API Key，如果不存在或为占位符则返回空字符串"""
    key = os.getenv("OPENAI_API_KEY", "")
    if is_placeholder_key(key):
        return ""
    return key


def get_jina_api_key() -> str:
    """获取 Jina API Key，如果不存在或为占位符则返回空字符串"""
    key = os.getenv("JINA_API_KEY", "")
    if is_placeholder_key(key):
        return ""
    return key


def require_live_services(*keys: str) -> bool:
    """
    检查所有必需的 API Key 是否可用
    
    Args:
        *keys: 需要检查的 API Key 列表
        
    Returns:
        bool: 如果所有 Key 都可用则返回 True
    """
    for key in keys:
        if not key or is_placeholder_key(key):
            return False
    return True


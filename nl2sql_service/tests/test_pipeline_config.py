"""
【简述】
验证 PipelineConfig 单例延迟初始化、缓存一致性以及环境变量覆盖机制的正确性。

【范围/不测什么】
- 不覆盖真实语义层加载或数据库连接；仅验证配置加载、单例模式与环境变量覆盖。
- 不覆盖并发安全（单进程假设）。

【用例概述】
- test_pipeline_config_lazy_init:
  -- 验证导入模块后缓存为 None，首次调用 get_pipeline_config() 才实例化
- test_pipeline_config_singleton_cached:
  -- 验证连续两次调用返回同一实例（id 相同）
- test_pipeline_config_env_override_before_first_call:
  -- 验证在首次调用前设置环境变量 VECTOR_SEARCH_TOP_K=77 时，配置值被正确覆盖
"""
import os
import pytest

from config import pipeline_config as config_module


@pytest.fixture(autouse=True)
def reset_pipeline_config():
    """
    每个测试前后重置 PipelineConfig 模块级缓存和环境变量，确保测试隔离。
    """
    # 保存原始环境变量值（如果存在）
    env_vars_to_clean = [
        "VECTOR_SEARCH_TOP_K",
        "MAX_TERM_RECALL",
        "SIMILARITY_THRESHOLD",
        "DEFAULT_LIMIT",
        "MAX_LIMIT_CAP",
        "EXECUTION_TIMEOUT_MS",
        "MAX_RESULT_ROWS",
        "MAX_LLM_ROWS",
    ]
    original_env = {}
    for var in env_vars_to_clean:
        if var in os.environ:
            original_env[var] = os.environ[var]
            del os.environ[var]
    
    # 重置模块级缓存
    config_module.pipeline_config = None
    
    yield
    
    # 恢复原始环境变量
    for var, value in original_env.items():
        os.environ[var] = value
    
    # 清理测试中设置的环境变量
    for var in env_vars_to_clean:
        if var in os.environ and var not in original_env:
            del os.environ[var]
    
    # 再次重置模块级缓存
    config_module.pipeline_config = None


@pytest.mark.unit
def test_pipeline_config_lazy_init():
    """
    【测试目标】
    1. 验证模块导入后缓存变量为 None（延迟初始化）
    2. 验证首次调用 get_pipeline_config() 时才创建实例

    【执行过程】
    1. 导入模块后检查 pipeline_config 变量为 None
    2. 首次调用 get_pipeline_config() 获取实例
    3. 验证返回的实例不为 None

    【预期结果】
    1. 导入后 config_module.pipeline_config is None
    2. 首次调用后返回 PipelineConfig 实例
    3. 实例的 vector_search_top_k 有默认值（30）
    """
    # 断言：导入后缓存为 None
    assert config_module.pipeline_config is None
    
    # 首次调用，应该创建实例
    config = config_module.get_pipeline_config()
    assert config is not None
    assert config.vector_search_top_k == 30  # 验证默认值


@pytest.mark.unit
def test_pipeline_config_singleton_cached():
    """
    【测试目标】
    1. 验证连续两次调用 get_pipeline_config() 返回同一实例（单例模式）

    【执行过程】
    1. 首次调用 get_pipeline_config() 获取实例1
    2. 再次调用 get_pipeline_config() 获取实例2
    3. 比较两个实例的 id

    【预期结果】
    1. 两次调用返回的实例 id 相同
    2. 实例是同一个对象（is 比较为 True）
    """
    config1 = config_module.get_pipeline_config()
    config2 = config_module.get_pipeline_config()
    
    # 断言：两次调用返回同一实例
    assert id(config1) == id(config2)
    assert config1 is config2


@pytest.mark.unit
def test_pipeline_config_env_override_before_first_call():
    """
    【测试目标】
    1. 验证在首次调用 get_pipeline_config() 前设置环境变量，配置值被正确覆盖

    【执行过程】
    1. 在首次调用前设置 os.environ["VECTOR_SEARCH_TOP_K"] = "77"
    2. 调用 get_pipeline_config() 获取配置实例
    3. 检查 vector_search_top_k 的值

    【预期结果】
    1. vector_search_top_k == 77（环境变量覆盖了默认值 30）
    """
    # 在首次调用前设置环境变量
    os.environ["VECTOR_SEARCH_TOP_K"] = "77"
    
    # 获取配置实例（此时会从环境变量读取）
    config = config_module.get_pipeline_config()
    
    # 断言：环境变量覆盖生效
    assert config.vector_search_top_k == 77


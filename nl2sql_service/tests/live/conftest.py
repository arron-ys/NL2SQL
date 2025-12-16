"""
Live Test Configuration

只在标记为 'live' 的测试中加载 .env 文件，使用真实的 API Key。
这样可以避免非 live 测试意外调用真实 API（有 token 成本）。

注意：这个 conftest.py 位于 tests/live/ 目录下，所以只会对 live 测试生效。
所有 live 测试都应该有 @pytest.mark.live 标记（由 tests/conftest.py 自动添加）。
"""
from pathlib import Path

import pytest
from dotenv import load_dotenv


@pytest.fixture(scope="session", autouse=True)
def load_env_for_live_tests(request):
    """
    自动加载 .env 文件的 fixture（仅对 live 测试生效）
    
    这个 fixture 会在所有 live 测试运行前加载 .env 文件。
    由于这个 conftest.py 位于 tests/live/ 目录下，它只会对 live 目录下的测试生效。
    
    Args:
        request: pytest request 对象，用于访问测试配置
    """
    # 获取项目根目录（nl2sql_service 目录）
    project_root = Path(__file__).parent.parent.parent
    
    # 加载 .env 文件
    # 注意：这个 fixture 只在 live 测试目录下，所以只会对 live 测试生效
    # override=False 确保不会覆盖已存在的环境变量
    env_file = project_root / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=False)
    else:
        # 如果 .env 文件不存在，记录警告但不失败（测试可能会跳过）
        import warnings
        warnings.warn(
            f".env file not found at {env_file}. Live tests may be skipped.",
            UserWarning
        )
    
    yield
    
    # 清理阶段（如果需要的话）

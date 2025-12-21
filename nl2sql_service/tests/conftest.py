"""
Pytest Configuration and Auto-Marking

根据测试文件路径自动为测试项添加 marker，避免手动标记遗漏。
强制校验：每个测试项必须至少拥有一个分层 marker。

注意：.env 文件只在标记为 'live' 或 'evaluation' 的测试中加载，避免非 live 测试使用真实 API（有 token 成本）。
"""
import os
import sys
import warnings
from pathlib import Path

# ============================================================
# 关键修复：在导入 pytest 之前设置编码，确保 pytest 捕获缓冲区使用 UTF-8
# ============================================================
# Windows 下 pytest 的捕获缓冲区默认使用系统编码（GBK），会导致 UnicodeDecodeError
# 必须在任何文件读取或输出之前设置
if "PYTHONIOENCODING" not in os.environ:
    os.environ["PYTHONIOENCODING"] = "utf-8"

# 设置 sys.stdout/stderr 编码（如果支持）
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass  # 某些环境可能不支持 reconfigure

import pytest

# 添加 nl2sql_service 目录到 Python 路径，确保可以从任何目录运行测试
# conftest.py 位于 nl2sql_service/tests/，所以 parent.parent 就是 nl2sql_service 目录
_nl2sql_service_dir = Path(__file__).parent.parent.resolve()

# 将 nl2sql_service 目录添加到 sys.path（如果尚未添加）
if str(_nl2sql_service_dir) not in sys.path:
    sys.path.insert(0, str(_nl2sql_service_dir))


# ============================================================
# Dotenv Loading Configuration
# ============================================================


def _should_load_dotenv(config) -> bool:
    """
    判断是否应该加载 .env 文件
    
    触发条件（满足任一即可）：
    1. 命令行参数 --load-dotenv
    2. markexpr 包含 live 或 evaluation
    3. 运行路径包含 tests/live 或 tests/evaluation
    """
    # 条件 1: 显式传参 --load-dotenv
    try:
        if config.getoption("--load-dotenv", default=False):
            return True
    except (ValueError, AttributeError):
        pass
    
    # 条件 2: markexpr 包含 live 或 evaluation
    try:
        markexpr = config.getoption("-m", default=None) or config.getoption("--markexpr", default=None)
        if markexpr:
            markexpr_lower = markexpr.lower()
            if "live" in markexpr_lower or "evaluation" in markexpr_lower:
                return True
    except (ValueError, AttributeError):
        pass
    
    # 条件 3: 运行路径包含 tests/live 或 tests/evaluation
    # 获取命令行参数（从 config.args 或通过 getoption）
    args_to_check = []
    try:
        # 尝试从 file_or_dir 选项获取
        file_or_dir = config.getoption("file_or_dir", default=[])
        if file_or_dir:
            args_to_check.extend(file_or_dir)
    except (ValueError, AttributeError):
        pass
    
    # 尝试从 config.args 获取（pytest 内部属性）
    if hasattr(config, "args") and config.args:
        args_to_check.extend(config.args)
    
    # 检查命令行参数中的路径
    for arg in args_to_check:
        arg_str = str(arg).replace("\\", "/").lower()
        # 兼容 Windows 和 Linux 路径分隔符
        # 检查是否包含 tests/live 或 tests/evaluation（包括子目录）
        if "tests/live" in arg_str or "tests\\live" in arg_str:
            return True
        if "tests/evaluation" in arg_str or "tests\\evaluation" in arg_str:
            return True
    
    return False


def _find_env_file() -> Path:
    """
    查找 .env 文件
    
    查找顺序（按存在优先）：
    1. <repo_root>/nl2sql_service/.env
    2. <repo_root>/.env
    
    返回找到的 .env 文件路径，如果都不存在则返回 None。
    """
    # 获取 repo 根目录（nl2sql_service 的父目录）
    repo_root = _nl2sql_service_dir.parent
    
    # 候选路径列表
    candidates = [
        _nl2sql_service_dir / ".env",  # nl2sql_service/.env
        repo_root / ".env",  # repo_root/.env
    ]
    
    # 返回第一个存在的文件
    for env_file in candidates:
        if env_file.exists() and env_file.is_file():
            return env_file
    
    return None


# 全局标志：标记 .env 是否已加载
_env_loaded = False


def _load_dotenv_if_needed(config, check_paths_from_items=None):
    """
    如果需要，加载 .env 文件（避免重复加载）
    
    Args:
        config: pytest config 对象
        check_paths_from_items: 可选的测试项列表，用于路径检测
    """
    global _env_loaded
    
    if _env_loaded:
        return
    
    should_load = False
    
    # 检查触发条件
    if _should_load_dotenv(config):
        should_load = True
    elif check_paths_from_items:
        # 补充检查：从测试项中检查路径
        for item in check_paths_from_items:
            if hasattr(item, "path") and item.path:
                item_path_str = str(item.path).replace("\\", "/").lower()
                if "tests/live" in item_path_str or "tests/evaluation" in item_path_str:
                    should_load = True
                    break
            # 也检查 nodeid
            nodeid_str = item.nodeid.replace("\\", "/").lower()
            if "tests/live" in nodeid_str or "tests/evaluation" in nodeid_str:
                should_load = True
                break
    
    if should_load:
        env_file = _find_env_file()
        if env_file:
            try:
                from dotenv import load_dotenv
                load_dotenv(dotenv_path=env_file, override=False)
                _env_loaded = True
            except ImportError:
                warnings.warn(
                    "python-dotenv is not installed. Cannot load .env file.",
                    UserWarning
                )
            except Exception as e:
                warnings.warn(
                    f"Failed to load .env file from {env_file}: {e}",
                    UserWarning
                )
        else:
            # 满足触发条件但找不到 .env 文件，只打印 warning，不报错
            warnings.warn(
                ".env file not found. Expected locations:\n"
                f"  1. {_nl2sql_service_dir / '.env'}\n"
                f"  2. {_nl2sql_service_dir.parent / '.env'}\n"
                "Live/evaluation tests may be skipped if API keys are not set via environment variables.",
                UserWarning
            )
            _env_loaded = True  # 标记为已处理，避免重复警告


def pytest_addoption(parser):
    """
    添加命令行选项
    """
    parser.addoption(
        "--load-dotenv",
        action="store_true",
        default=False,
        help="Force loading .env file regardless of test markers or paths"
    )


def pytest_configure(config):
    """
    在测试收集前配置 pytest
    
    根据触发条件加载 .env 文件。
    同时为非 live 测试设置 QDRANT_MODE=memory（必须在 import 之前设置）。
    设置 PYTHONIOENCODING=utf-8 确保 pytest 捕获缓冲区使用 UTF-8 编码（修复 Windows 下 UnicodeDecodeError）。
    """
    # 关键修复：设置 Python IO 编码为 UTF-8，确保 pytest 捕获缓冲区使用 UTF-8
    # 这必须在任何文件读取或输出之前设置
    if "PYTHONIOENCODING" not in os.environ:
        os.environ["PYTHONIOENCODING"] = "utf-8"
    
    # 同时设置 sys.stdout/stderr 编码（如果支持）
    if sys.platform == "win32":
        try:
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8')
            if hasattr(sys.stderr, 'reconfigure'):
                sys.stderr.reconfigure(encoding='utf-8')
        except Exception:
            pass  # 某些环境可能不支持 reconfigure
    
    _load_dotenv_if_needed(config)
    
    # 在 pytest_configure 阶段就设置 VECTOR_STORE_MODE=memory（如果当前没有 live marker）
    # 注意：此时还没有测试项，我们只能根据命令行参数判断
    try:
        markexpr = config.getoption("-m", default=None) or config.getoption("--markexpr", default=None)
        if markexpr and "live" not in markexpr.lower():
            # 非 live 测试：强制 memory 模式
            os.environ["VECTOR_STORE_MODE"] = "memory"
    except (ValueError, AttributeError):
        pass


# 定义必须至少拥有 1 个的"分层 marker"集合
LAYER_MARKERS = {
    "unit",
    "integration",
    "e2e",
    "regression",
    "security",
    "stability",
    "observability",
    "performance",
    "quality",
}

# 路径到 marker 的映射（key 使用相对 tests 根目录的路径，例如 "test_schemas.py" 或 "live/test_e2e_live.py"）
path_marker_map = {
    # 根目录测试文件
    "test_schemas.py": ["unit"],
    "test_validation.py": ["unit"],
    "test_registry.py": ["unit"],
    "test_ai_client.py": ["unit"],
    "test_jina_proxy_and_stage2_vector_required.py": ["unit"],
    "test_logger_and_middleware.py": ["unit"],
    "test_qdrant_init_clients.py": ["unit"],
    "test_plan_api.py": ["integration"],
    "test_plan_error_response.py": ["integration"],
    "test_execute_error_response.py": ["integration"],
    "test_plan_regression.py": ["regression", "integration"],
    "test_e2e_pipeline.py": ["e2e"],
    "test_security.py": ["security"],
    "test_stability.py": ["stability"],
    "test_observability.py": ["observability"],
    "test_performance_internal.py": ["performance"],  # 内部性能测试（Mock 版）
    "test_quality_evaluation.py": ["quality", "slow"],
    "test_stage2_plan_normalization.py": ["unit"],
    "test_stage3_permission_denied_on_warning.py": ["unit"],
    "test_plan_permission_denied_soft_error.py": ["integration"],
    "test_execute_permission_denied_goes_to_stage6.py": ["integration"],
    "test_prompt_templates.py": ["unit"],
    "test_ai_client_fallback.py": ["unit"],
    "test_default_inference.py": ["unit"],
    "test_dialect_differences.py": ["unit"],
    "test_resource_cleanup.py": ["unit"],
    "test_db_detect_upgrade.py": ["integration"],
    # Live 测试文件（使用完整相对路径避免冲突）
    # 注意：live 测试只允许有 live marker，不允许 unit/integration
    "live/test_e2e_live.py": ["e2e", "slow", "live"],
    "live/test_db_connection.py": ["live"],
    "live/test_api_execute.py": ["live"],
    # Evaluation 测试文件（性能评估）
    "evaluation/test_performance_live.py": ["performance", "slow", "live"],
}


def _get_tests_root_path() -> Path:
    """
    获取 tests 根目录的路径
    
    通过查找包含 conftest.py 的目录来确定。
    """
    # 获取当前 conftest.py 所在目录（即 tests 根目录）
    current_file = Path(__file__).resolve()
    return current_file.parent


def _get_relative_test_path(item) -> str:
    """
    获取测试文件相对于 tests 根目录的路径
    
    优先使用 item.path，fallback 到 nodeid 解析。
    返回格式：例如 "test_schemas.py" 或 "live/test_e2e_live.py"
    """
    tests_root = _get_tests_root_path()
    
    # 优先使用 item.path（pathlib.Path）
    if hasattr(item, "path") and item.path:
        try:
            item_path = Path(item.path).resolve()
            # 计算相对于 tests 根目录的路径
            try:
                relative_path = item_path.relative_to(tests_root)
                # 转换为字符串，统一使用正斜杠
                return str(relative_path).replace("\\", "/")
            except ValueError:
                # 如果不在 tests 根目录下，fallback 到 basename
                return item_path.name
        except (AttributeError, TypeError):
            pass
    
    # Fallback: 从 nodeid 解析
    file_path = item.nodeid.split("::")[0]
    # 统一分隔符
    file_path = file_path.replace("\\", "/")
    
    # 尝试从 nodeid 中提取相对路径
    # nodeid 可能是 "tests/test_schemas.py" 或 "nl2sql_service/tests/test_schemas.py"
    parts = file_path.split("/")
    
    # 查找 "tests" 目录
    if "tests" in parts:
        tests_index = parts.index("tests")
        # 获取 tests 之后的所有部分
        relative_parts = parts[tests_index + 1:]
        if relative_parts:
            return "/".join(relative_parts)
    
    # 如果找不到 tests 目录，返回最后一个部分（basename）
    return parts[-1] if parts else file_path


def _get_file_name(item) -> str:
    """
    获取测试项所属的文件名（basename）
    
    用于向后兼容和 fallback。
    """
    relative_path = _get_relative_test_path(item)
    return os.path.basename(relative_path)


def pytest_collection_modifyitems(config, items):
    """
    根据测试文件路径自动添加 marker，并强制校验每个测试项都有分层 marker。
    
    规则：
    1. 优先使用相对 tests 根目录的路径匹配映射表，避免路径前缀问题
    2. 如果 item 已经有该 marker，则不要重复添加
    3. 自动打标后，强制校验每个 item 必须至少拥有一个分层 marker
    4. 对于 test_ 开头的文件，如果既不在 map 中又没有任何分层 marker，则报错
    """
    # 补充检查：如果之前没有加载 .env，现在根据测试项路径再次检查
    _load_dotenv_if_needed(config, check_paths_from_items=items)
    
    
    # 收集所有未归类的测试项（用于错误报告）
    unmarked_items = []
    
    for item in items:
        # 获取相对路径和文件名
        relative_path = _get_relative_test_path(item)
        file_name = _get_file_name(item)
        
        # 获取现有 markers
        existing_markers = {m.name for m in item.iter_markers()}
        
        # 优先使用相对路径匹配，fallback 到文件名匹配
        markers_to_add = None
        if relative_path in path_marker_map:
            markers_to_add = path_marker_map[relative_path]
        elif file_name in path_marker_map:
            markers_to_add = path_marker_map[file_name]
        
        # 如果文件在映射表中，自动添加 marker
        if markers_to_add:
            for marker_name in markers_to_add:
                # 避免重复添加
                if marker_name not in existing_markers:
                    marker = getattr(pytest.mark, marker_name)
                    item.add_marker(marker)
                    # 更新 existing_markers（用于后续校验）
                    existing_markers.add(marker_name)
        
        # ============================================================
        # 强制 live 目录语义：live 测试不允许属于 unit/integration
        # ============================================================
        # 检查是否在 live 目录下（兼容 Windows 和 Linux 路径）
        nodeid_str = item.nodeid.replace("\\", "/").lower()
        relative_path_lower = relative_path.replace("\\", "/").lower()
        is_live_test = (
            "tests/live/" in nodeid_str or
            "tests\\live\\" in nodeid_str or
            relative_path_lower.startswith("live/") or
            relative_path_lower.startswith("live\\")
        )
        
        if is_live_test:
            # 强制追加 live marker（如果还没有）
            if "live" not in existing_markers:
                item.add_marker(pytest.mark.live)
                existing_markers.add("live")
            
            # 检查并移除污染：如果 live 测试同时拥有 unit 或 integration marker，直接失败
            conflicting_markers = []
            for marker in item.iter_markers():
                if marker.name in ("unit", "integration"):
                    conflicting_markers.append(marker.name)
                    # 移除冲突的 marker
                    item.own_markers = [m for m in item.own_markers if m.name != marker.name]
            
            if conflicting_markers:
                # 从 nodeid 中提取更清晰的路径信息
                file_path = str(item.path) if hasattr(item, "path") and item.path else item.nodeid
                raise AssertionError(
                    f"\n{'=' * 80}\n"
                    f"ERROR: Live test has conflicting markers\n"
                    f"{'=' * 80}\n"
                    f"Test: {item.nodeid}\n"
                    f"File: {file_path}\n"
                    f"Conflicting markers: {', '.join(conflicting_markers)}\n"
                    f"\n"
                    f"Live tests are NOT allowed to have 'unit' or 'integration' markers.\n"
                    f"Please remove @pytest.mark.{conflicting_markers[0]} (and any other conflicting markers)\n"
                    f"from the test or from conftest.py path_marker_map.\n"
                    f"{'=' * 80}\n"
                )
        
        # 强制校验：检查是否至少拥有一个分层 marker
        # 例外：live 测试不需要分层 marker（它们有自己的 live marker）
        has_live_marker = "live" in existing_markers
        has_layer_marker = any(m.name in LAYER_MARKERS for m in item.iter_markers())
        
        if not has_layer_marker and not has_live_marker:
            # 收集未归类的测试项信息
            item_path = str(item.path) if hasattr(item, "path") and item.path else "N/A"
            # 获取当前所有 markers（包括非分层的）
            current_markers = sorted(existing_markers) if existing_markers else ["无"]
            
            # 判断是否为 test_ 开头的文件
            is_test_file = file_name.startswith("test_") and file_name.endswith(".py")
            in_map = relative_path in path_marker_map or file_name in path_marker_map
            
            unmarked_items.append({
                "nodeid": item.nodeid,
                "file_path": item_path,
                "relative_path": relative_path,
                "file_name": file_name,
                "current_markers": current_markers,
                "is_test_file": is_test_file,
                "in_map": in_map,
            })
    
    # 如果有未归类的测试项，在收集阶段直接失败
    if unmarked_items:
        error_lines = [
            "\n" + "=" * 80,
            "ERROR: Found test items without layer markers",
            "=" * 80,
        ]
        
        for info in unmarked_items:
            error_lines.append(f"\n  Test: {info['nodeid']}")
            error_lines.append(f"  File: {info['file_path']}")
            error_lines.append(f"  Relative path: {info['relative_path']}")
            error_lines.append(f"  File name: {info['file_name']}")
            error_lines.append(f"  Current markers: {', '.join(info['current_markers'])}")
            
            # 针对 test_ 开头的文件给出更明确的提示
            if info['is_test_file'] and not info['in_map']:
                error_lines.append(f"\n  ⚠️  新增测试文件 '{info['relative_path']}' 未配置 marker")
                error_lines.append(f"  修复方式（二选一）：")
                error_lines.append(f"    1) 在 conftest.py 的 path_marker_map 中添加：")
                error_lines.append(f"       \"{info['relative_path']}\": [\"unit\"],  # 或其他分层 marker")
                error_lines.append(f"    2) 在测试文件中手动添加 marker：")
                error_lines.append(f"       @pytest.mark.unit  # 或其他分层 marker")
            else:
                error_lines.append(f"\n  修复方式（二选一）：")
                error_lines.append(f"    1) 在测试项上添加 @pytest.mark.<layer>")
                error_lines.append(f"    2) 在 conftest.py 的 path_marker_map 中添加：")
                error_lines.append(f"       \"{info['relative_path']}\": [\"unit\"],  # 或其他分层 marker")
        
        error_lines.append("\n" + "=" * 80)
        error_lines.append(
            f"必需的分层 markers: {', '.join(sorted(LAYER_MARKERS))}"
        )
        error_lines.append("=" * 80 + "\n")
        
        raise pytest.UsageError("".join(error_lines))


# ============================================================
# Offline Mode Fixture (for unit/integration tests)
# ============================================================

@pytest.fixture(autouse=True)
def enforce_offline_mode(request):
    """
    自动应用的 fixture：为非 live 测试强制设置 NO_NETWORK=1 和 QDRANT_MODE=memory
    
    这确保 unit/integration 测试不会意外访问真实网络，且不会创建文件锁。
    live 测试不受此限制。
    """
    # 检查当前测试是否有 live marker
    has_live_marker = any(m.name == "live" for m in request.node.iter_markers())
    
    if not has_live_marker:
        # 非 live 测试：设置 NO_NETWORK=1 和 VECTOR_STORE_MODE=memory
        original_no_network = os.environ.get("NO_NETWORK")
        original_vector_store_mode = os.environ.get("VECTOR_STORE_MODE")
        os.environ["NO_NETWORK"] = "1"
        os.environ["VECTOR_STORE_MODE"] = "memory"  # 避免文件锁
        yield
        # 恢复原始值
        if original_no_network is None:
            os.environ.pop("NO_NETWORK", None)
        else:
            os.environ["NO_NETWORK"] = original_no_network
        if original_vector_store_mode is None:
            os.environ.pop("VECTOR_STORE_MODE", None)
        else:
            os.environ["VECTOR_STORE_MODE"] = original_vector_store_mode
    else:
        # live 测试：确保 NO_NETWORK 未设置，但保留 QDRANT_MODE（如果有）
        original_no_network = os.environ.get("NO_NETWORK")
        os.environ.pop("NO_NETWORK", None)
        yield
        # 恢复原始值
        if original_no_network is not None:
            os.environ["NO_NETWORK"] = original_no_network


# ============================================================
# Standard Test Client Fixtures
# ============================================================

from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

# 延迟导入 app，避免循环依赖
import sys
from pathlib import Path as PathLib
_nl2sql_service_dir = PathLib(__file__).parent.parent.resolve()
if str(_nl2sql_service_dir) not in sys.path:
    sys.path.insert(0, str(_nl2sql_service_dir))


@pytest.fixture
def client():
    """
    同步 TestClient fixture
    
    使用 context manager 确保 FastAPI lifespan 事件正确触发。
    适用于同步测试。
    """
    from main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
async def async_client():
    """
    异步 AsyncClient fixture（会话级别）
    
    使用 ASGITransport 和 AsyncClient 确保 FastAPI lifespan 事件正确触发。
    适用于异步测试（@pytest.mark.asyncio）。
    
    注意：
    - scope="session" 确保所有测试共享同一个 app 实例和 Qdrant 客户端，
      避免重复初始化导致的文件锁冲突（Qdrant 本地存储不支持多实例并发访问）。
    - 只在第一个测试时初始化 lifespan，在所有测试结束后才关闭。
    """
    from main import app
    from contextlib import asynccontextmanager
    
    # 手动触发 lifespan startup
    async with app.router.lifespan_context(app):
        # 在 lifespan 上下文中创建 client
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as ac:
            yield ac


# ============================================================
# Mock Fixtures
# ============================================================

from unittest.mock import MagicMock


@pytest.fixture
def mock_registry():
    """
    创建模拟的 SemanticRegistry
    
    统一的mock_registry fixture，提供标准的registry mock配置。
    所有测试文件应使用此fixture，避免重复定义。
    """
    registry = MagicMock()
    registry.get_allowed_ids.return_value = {
        "METRIC_GMV",
        "METRIC_REVENUE",
        "DIM_REGION",
        "DIM_DEPARTMENT",
    }
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENTITY_ORDER",
        "default_filters": [],
        "default_time": None,
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_REGION",
        "entity_id": "ENTITY_ORDER",
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {
        "global_settings": {},
        "time_windows": [],
    }
    return registry


# ============================================================
# Test Helper Functions
# ============================================================

def format_request_summary(request_payload: dict, trace_id: str = None, response: dict = None) -> str:
    """
    格式化请求摘要，用于测试失败时的诊断信息（脱敏处理）
    
    Args:
        request_payload: 请求payload字典
        trace_id: 可选的trace_id
        response: 可选的响应字典（用于包含request_id）
    
    Returns:
        格式化的摘要字符串（已脱敏）
    """
    summary_parts = []
    
    # 请求摘要（脱敏）
    if request_payload:
        sanitized_payload = {}
        for key, value in request_payload.items():
            if key in ["user_id", "tenant_id"]:
                # 只显示前3个字符，其余用*替代
                if isinstance(value, str) and len(value) > 3:
                    sanitized_payload[key] = value[:3] + "*" * (len(value) - 3)
                else:
                    sanitized_payload[key] = value
            elif key == "question":
                # 问题只显示前50个字符
                if isinstance(value, str) and len(value) > 50:
                    sanitized_payload[key] = value[:50] + "..."
                else:
                    sanitized_payload[key] = value
            else:
                sanitized_payload[key] = value
        summary_parts.append(f"Request: {sanitized_payload}")
    
    # Trace ID / Request ID
    if trace_id:
        summary_parts.append(f"Trace-ID: {trace_id}")
    if response and isinstance(response, dict):
        request_id = response.get("request_id")
        if request_id:
            summary_parts.append(f"Request-ID: {request_id}")
    
    return " | ".join(summary_parts) if summary_parts else "No request info"


def assert_with_context(
    condition: bool,
    message: str,
    request_payload: dict = None,
    trace_id: str = None,
    response: dict = None
):
    """
    带上下文的断言，失败时输出请求摘要
    
    Args:
        condition: 断言条件
        message: 断言失败消息
        request_payload: 请求payload（用于诊断）
        trace_id: trace_id（用于诊断）
        response: 响应字典（用于提取request_id）
    """
    if not condition:
        context = format_request_summary(request_payload, trace_id, response)
        raise AssertionError(f"{message} | Context: {context}")

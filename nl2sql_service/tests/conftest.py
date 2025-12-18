"""
Pytest Configuration and Auto-Marking

根据测试文件路径自动为测试项添加 marker，避免手动标记遗漏。
强制校验：每个测试项必须至少拥有一个分层 marker。

注意：.env 文件只在标记为 'live' 的测试中加载，避免非 live 测试使用真实 API（有 token 成本）。
"""
import os
from pathlib import Path

import pytest


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
    "test_execute_error_response_debug.py": ["integration"],
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
    # Live 测试文件（使用完整相对路径避免冲突）
    "live/test_performance_live.py": ["performance", "slow", "live"],
    "live/test_e2e_live.py": ["e2e", "slow", "live"],
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
        
        # 强制校验：检查是否至少拥有一个分层 marker
        has_layer_marker = any(m.name in LAYER_MARKERS for m in item.iter_markers())
        
        if not has_layer_marker:
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

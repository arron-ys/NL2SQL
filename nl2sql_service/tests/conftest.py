"""
Pytest Configuration and Auto-Marking

根据测试文件路径自动为测试项添加 marker，避免手动标记遗漏。
强制校验：每个测试项必须至少拥有一个分层 marker。
"""
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

# 文件到 marker 的映射（key 使用纯文件名）
file_marker_map = {
    "test_schemas.py": ["unit"],
    "test_validation.py": ["unit"],
    "test_registry.py": ["unit"],
    "test_ai_client.py": ["unit"],
    "test_logger_and_middleware.py": ["unit"],
    "test_qdrant_init_clients.py": ["unit"],
    "test_plan_api.py": ["integration"],
    "test_plan_regression.py": ["regression", "integration"],  # 如果已手动标记，不会重复添加
    "test_e2e_pipeline.py": ["e2e"],
    "test_security.py": ["security"],
    "test_stability.py": ["stability"],
    "test_observability.py": ["observability"],
    "test_performance.py": ["performance", "slow"],
    "test_quality_evaluation.py": ["quality", "slow"],
}


def _get_file_name(item) -> str:
    """
    获取测试项所属的文件名（basename）
    
    优先使用 item.path.name，fallback 到 nodeid 解析。
    """
    # 优先使用 item.path（pathlib.Path）
    if hasattr(item, "path") and item.path:
        return item.path.name
    
    # Fallback: 从 nodeid 解析
    file_path = item.nodeid.split("::")[0]
    # 统一分隔符后取 basename
    file_name = file_path.replace("\\", "/").split("/")[-1]
    return file_name


def pytest_collection_modifyitems(config, items):
    """
    根据测试文件路径自动添加 marker，并强制校验每个测试项都有分层 marker。
    
    规则：
    1. 使用文件名（basename）匹配映射表，避免路径前缀问题
    2. 如果 item 已经有该 marker，则不要重复添加
    3. 自动打标后，强制校验每个 item 必须至少拥有一个分层 marker
    4. 对于 test_ 开头的文件，如果既不在 map 中又没有任何分层 marker，则报错
    """
    # 收集所有未归类的测试项（用于错误报告）
    unmarked_items = []
    
    for item in items:
        # 获取文件名
        file_name = _get_file_name(item)
        
        # 获取现有 markers
        existing_markers = {m.name for m in item.iter_markers()}
        
        # 如果文件在映射表中，自动添加 marker
        if file_name in file_marker_map:
            markers_to_add = file_marker_map[file_name]
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
            in_map = file_name in file_marker_map
            
            unmarked_items.append({
                "nodeid": item.nodeid,
                "file_path": item_path,
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
            error_lines.append(f"  File name: {info['file_name']}")
            error_lines.append(f"  Current markers: {', '.join(info['current_markers'])}")
            
            # 针对 test_ 开头的文件给出更明确的提示
            if info['is_test_file'] and not info['in_map']:
                error_lines.append(f"\n  ⚠️  新增测试文件 '{info['file_name']}' 未配置 marker")
                error_lines.append(f"  修复方式（二选一）：")
                error_lines.append(f"    1) 在 conftest.py 的 file_marker_map 中添加：")
                error_lines.append(f"       \"{info['file_name']}\": [\"unit\"],  # 或其他分层 marker")
                error_lines.append(f"    2) 在测试文件中手动添加 marker：")
                error_lines.append(f"       @pytest.mark.unit  # 或其他分层 marker")
            else:
                error_lines.append(f"\n  修复方式（二选一）：")
                error_lines.append(f"    1) 在测试项上添加 @pytest.mark.<layer>")
                error_lines.append(f"    2) 在 conftest.py 的 file_marker_map 中添加：")
                error_lines.append(f"       \"{info['file_name']}\": [\"unit\"],  # 或其他分层 marker")
        
        error_lines.append("\n" + "=" * 80)
        error_lines.append(
            f"必需的分层 markers: {', '.join(sorted(LAYER_MARKERS))}"
        )
        error_lines.append("=" * 80 + "\n")
        
        raise pytest.UsageError("".join(error_lines))

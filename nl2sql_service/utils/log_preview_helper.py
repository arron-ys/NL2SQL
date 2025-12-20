"""
Log Preview Helper Module

提供用于日志输出的文本和 JSON 预览工具，确保日志内容可控、可读且包含指纹信息。

设计原则：
1. 只做格式化/截断/加指纹，不做业务变换
2. 必须包含长度、hash 等指纹信息，便于定位和对比
3. 必须支持可控截断，避免日志爆炸
"""

import hashlib
import json
from typing import Any


def preview_text(text: str, head: int = 300, *, label: str | None = None) -> str:
    """
    生成文本预览，包含长度、hash 和预览内容。
    
    用途：
    - 用于打印 SQL、子查询描述等文本型产出物
    - 通过 hash 可以唯一标识完整内容，便于定位和对比
    - 通过 head 参数控制预览长度，避免日志过长
    
    参数：
        text: 要预览的文本内容
        head: 预览文本的前 N 个字符（默认 300）
              建议值：
              - 子查询描述/问题文本：300（适中长度）
              - SQL 语句：1200（SQL 通常需要更长预览才能看清结构）
              可根据实际情况调整，但建议不超过 2000 以避免日志过长
        label: 可选标签，用于区分不同类型的文本（不影响输出格式）
    
    返回：
        格式化的预览字符串，包含：
        - len=<总长度>
        - hash=<sha1前8位>
        - preview=<前head字符，换行符替换为\\n>
    
    示例：
        >>> preview_text("SELECT * FROM table\\nWHERE id=1", head=20)
        'len=28 hash=a1b2c3d4 preview="SELECT * FROM table\\nWHE..."'
    """
    if not isinstance(text, str):
        text = str(text)
    
    # 计算 SHA1 hash（前8位）
    text_bytes = text.encode('utf-8')
    hash_obj = hashlib.sha1(text_bytes)
    hash_hex = hash_obj.hexdigest()[:8]
    
    text_len = len(text)
    
    # 替换换行符为 \n（避免日志断行失控）
    # 这样可以在单行日志中看到换行位置，同时不会导致日志系统误判为多行
    text_normalized = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # 截断预览
    if text_len <= head:
        preview = text_normalized
    else:
        preview = text_normalized[:head] + "...(truncated)"
    
    # 构建输出字符串
    parts = [
        f"len={text_len}",
        f"hash={hash_hex}",
        f'preview="{preview}"'
    ]
    
    if label:
        parts.insert(0, f"[{label}]")
    
    return " ".join(parts)


def preview_json(
    obj: Any,
    *,
    max_lines: int = 80,
    max_chars: int = 2000,
    label: str | None = None
) -> str:
    """
    生成 JSON 对象的预览，包含长度、hash 和格式化的 JSON 文本。
    
    用途：
    - 用于打印 Plan、配置等结构化产出物
    - 通过 hash 可以唯一标识完整 JSON，便于定位和对比
    - 通过 max_lines 和 max_chars 控制预览大小
    
    参数：
        obj: 要预览的任意对象（会被序列化为 JSON）
        max_lines: 最大行数限制（默认 80）
                   优先按行数截断：如果格式化后的 JSON 行数超过此值，则在第 max_lines 行截断
                   建议值：
                   - Plan 对象：80（能看清完整结构）
                   - 小型配置：50
                   - 可根据实际情况调整，但建议不超过 150
        max_chars: 最大字符数限制（默认 2000）
                   如果按行截断后仍超过此字符数，则按字符数截断
                   建议值：1500-3000 之间，根据日志系统性能调整
        label: 可选标签，用于区分不同类型的 JSON（不影响输出格式）
    
    返回：
        格式化的预览字符串，包含：
        - len=<原始JSON字符串总长度>
        - hash=<sha1前8位>
        - json=<格式化的JSON文本（可能被截断）>
    
    截断规则：
        1. 优先按 max_lines 截断行数（保持 JSON 结构完整性）
        2. 如果按行截断后仍超过 max_chars，再按字符数截断
        3. 这样设计的原因：
           - 按行截断可以保留完整的 JSON 结构（不会在对象中间断开）
           - 按字符截断作为兜底，防止单行过长导致日志系统问题
    
    为什么使用 default=str：
        - 某些对象（如 datetime、enum 等）无法直接 JSON 序列化
        - default=str 会将无法序列化的对象转换为字符串，确保不会抛出异常
        - 这样可以安全地序列化 Pydantic 模型、枚举等复杂对象
    
    示例：
        >>> preview_json({"a": 1, "b": "text"}, max_lines=10)
        'len=15 hash=a1b2c3d4 json={"a": 1, "b": "text"}'
    """
    # 序列化为 JSON 字符串
    # 使用 ensure_ascii=False 支持中文
    # 使用 indent=2 格式化（便于阅读）
    # 使用 default=str 处理无法序列化的对象（如 datetime、enum）
    try:
        json_str = json.dumps(obj, ensure_ascii=False, indent=2, default=str)
    except (TypeError, ValueError) as e:
        # 如果序列化失败，使用 repr 作为兜底
        json_str = json.dumps({"__serialization_error__": str(e), "__repr__": repr(obj)})
    
    json_len = len(json_str)
    
    # 计算 SHA1 hash（前8位）
    json_bytes = json_str.encode('utf-8')
    hash_obj = hashlib.sha1(json_bytes)
    hash_hex = hash_obj.hexdigest()[:8]
    
    # 按行截断
    lines = json_str.split('\n')
    if len(lines) > max_lines:
        # 截断到 max_lines 行，并添加截断标记
        truncated_lines = lines[:max_lines]
        # 尝试保持 JSON 结构：如果截断后最后一行不完整，尝试补充闭合括号
        last_line = truncated_lines[-1]
        # 简单策略：如果最后一行看起来不完整（有未闭合的括号），添加注释
        if last_line.strip() and not last_line.strip().endswith(('}', ']', ',')):
            truncated_lines.append('  // ... (truncated by max_lines)')
        else:
            truncated_lines.append('  ... (truncated by max_lines)')
        json_preview = '\n'.join(truncated_lines)
    else:
        json_preview = json_str
    
    # 按字符截断（如果仍超过限制）
    if len(json_preview) > max_chars:
        # 尝试在行边界截断
        truncated_chars = json_preview[:max_chars]
        # 找到最后一个完整行
        last_newline = truncated_chars.rfind('\n')
        if last_newline > max_chars - 100:  # 如果最后一行不太长，在行边界截断
            json_preview = truncated_chars[:last_newline] + '\n  ... (truncated by max_chars)'
        else:
            json_preview = truncated_chars + ' ... (truncated by max_chars)'
    
    # 构建输出字符串
    # 将 JSON 缩进，便于在日志中阅读
    json_indented = '\n'.join('  ' + line if line.strip() else line for line in json_preview.split('\n'))
    
    parts = [
        f"len={json_len}",
        f"hash={hash_hex}",
        f"json=\n{json_indented}"
    ]
    
    if label:
        parts.insert(0, f"[{label}]")
    
    return "\n".join(parts)


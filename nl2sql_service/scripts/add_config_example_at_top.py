"""
在 .env.example 文件开头添加清晰的配置示例
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 查找文件开头的注释结束位置（通常在 "请根据实际情况填写以下配置项" 之后）
insert_pos = 0
for i, line in enumerate(lines):
    if "请根据实际情况填写以下配置项" in line or "注意：此文件包含敏感信息" in line:
        # 找到数据库配置部分之前插入示例
        for j in range(i, min(i+10, len(lines))):
            if "数据库配置" in lines[j] or "Database Configuration" in lines[j]:
                insert_pos = j
                break
        if insert_pos > 0:
            break

if insert_pos == 0:
    # 如果找不到，在第 10 行之后插入
    insert_pos = 10

# 要插入的配置示例
example_section = """
# ============================================================
# 快速配置示例：切换到 DeepSeek
# ============================================================
# 如果你想要使用 DeepSeek 作为默认 LLM Provider，请按以下步骤配置：
#
# 1. 明确指定使用 DeepSeek（取消下面的注释）
# DEFAULT_LLM_PROVIDER=deepseek
#
# 2. 在下面的 "DeepSeek 配置" 部分填入你的 DEEPSEEK_API_KEY
#
# 3. （可选）配置 DeepSeek 的模型和超时设置
#
# 完整的 DeepSeek 配置示例请参考下面的 "DeepSeek 配置" 部分
# ============================================================

"""

# 检查是否已经添加过
if "快速配置示例：切换到 DeepSeek" not in ''.join(lines):
    lines.insert(insert_pos, example_section)
    
    # 写入文件
    with open(env_example_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print(f"Successfully added example section at line {insert_pos + 1}")
else:
    print("Example section already exists")

print(f"File updated: {env_example_path}")

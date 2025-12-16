"""
在 .env.example 文件的 DeepSeek 配置部分添加清晰的示例
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
i = 0
found_deepseek_section = False
added_example = False

while i < len(lines):
    line = lines[i]
    new_lines.append(line)
    
    # 在 DeepSeek 配置部分的开头添加完整示例
    if '# ============================================================' in line and 'DeepSeek' in lines[i+1] if i+1 < len(lines) else False:
        found_deepseek_section = True
    
    # 在 DeepSeek API Key 之前添加完整的使用示例
    if found_deepseek_section and 'DEEPSEEK_API_KEY=' in line and not added_example:
        # 在 API Key 之前插入完整示例
        new_lines.insert(-1, '\n')
        new_lines.insert(-1, '# ============================================================\n')
        new_lines.insert(-1, '# 切换到 DeepSeek 的完整配置示例\n')
        new_lines.insert(-1, '# ============================================================\n')
        new_lines.insert(-1, '# 1. 明确指定使用 DeepSeek 作为默认 LLM Provider\n')
        new_lines.insert(-1, 'DEFAULT_LLM_PROVIDER=deepseek\n')
        new_lines.insert(-1, '\n')
        new_lines.insert(-1, '# 2. 配置 DeepSeek API Key（必需）\n')
        added_example = True
    
    i += 1

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Successfully updated {env_example_path}")
print("Added clear DeepSeek configuration example")

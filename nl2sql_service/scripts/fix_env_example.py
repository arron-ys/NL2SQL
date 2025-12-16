"""
修复 .env.example 文件，确保 DEFAULT_LLM_PROVIDER 等配置项清晰可见
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
i = 0
while i < len(lines):
    line = lines[i]
    
    # 取消注释 DEFAULT_LLM_PROVIDER（如果被注释了）
    if '# DEFAULT_LLM_PROVIDER=deepseek' in line:
        # 保留注释说明，但添加一个未注释的示例
        new_lines.append(line)  # 保留注释行
        new_lines.append('# 示例：明确指定使用 DeepSeek\n')
        new_lines.append('# DEFAULT_LLM_PROVIDER=deepseek\n')
        new_lines.append('# 示例：明确指定使用 OpenAI\n')
        new_lines.append('# DEFAULT_LLM_PROVIDER=openai\n')
        new_lines.append('# 示例：明确指定使用 Qwen\n')
        new_lines.append('# DEFAULT_LLM_PROVIDER=qwen\n')
        new_lines.append('\n')
    else:
        new_lines.append(line)
    
    i += 1

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Successfully updated {env_example_path}")
print("Added clear examples for DEFAULT_LLM_PROVIDER")

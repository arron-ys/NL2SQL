"""
在 .env.example 文件中添加 DEFAULT_LLM_PROVIDER 配置项
"""
from pathlib import Path

# 获取项目根目录
project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

# 读取现有内容
with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 查找插入位置并添加配置
new_lines = []
i = 0
while i < len(lines):
    line = lines[i]
    new_lines.append(line)
    
    # 在通用超时配置之后添加 DEFAULT_LLM_PROVIDER
    if 'LLM_TIMEOUT=60.0' in line and 'DEFAULT_LLM_PROVIDER' not in ''.join(new_lines):
        new_lines.append('\n')
        new_lines.append('# 默认 LLM Provider 配置 (可选)\n')
        new_lines.append('# 可选值: openai, deepseek, qwen\n')
        new_lines.append('# 如果不设置，将按以下优先级自动选择: DeepSeek > Qwen > OpenAI\n')
        new_lines.append('# DEFAULT_LLM_PROVIDER=deepseek\n')
        new_lines.append('\n')
    
    i += 1

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Successfully updated {env_example_path}")
print("Added DEFAULT_LLM_PROVIDER configuration")

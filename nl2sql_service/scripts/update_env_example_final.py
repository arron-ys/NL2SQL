"""
更新 .env.example 文件，添加 DeepSeek、Qwen 模型配置和通用超时配置
"""
from pathlib import Path

# 获取项目根目录
project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

# 读取现有内容
with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 查找插入位置
new_lines = []
i = 0
while i < len(lines):
    line = lines[i]
    new_lines.append(line)
    
    # 在 LLM 配置部分开头添加通用超时配置
    if '# LLM 配置 (LLM Configuration)' in line and 'LLM_TIMEOUT' not in ''.join(new_lines):
        new_lines.append('\n')
        new_lines.append('# ============================================================\n')
        new_lines.append('# 通用超时配置 (General Timeout Configuration)\n')
        new_lines.append('# ============================================================\n')
        new_lines.append('# 通用 LLM 超时配置（所有 LLM provider 都使用，除非设置了 provider 特定超时）\n')
        new_lines.append('# 默认值：60.0 秒（OpenAI/DeepSeek/Qwen），30.0 秒（Jina）\n')
        new_lines.append('LLM_TIMEOUT=60.0\n')
        new_lines.append('\n')
    
    # 在 OpenAI_TIMEOUT 之后更新注释
    if 'OPENAI_TIMEOUT=60.0' in line and '# OPENAI_TIMEOUT' not in ''.join(new_lines[-5:]):
        new_lines[-1] = '# OPENAI_TIMEOUT=60.0  # 可选，如果设置了会覆盖 LLM_TIMEOUT\n'
    
    # 在 DeepSeek Base URL 之后添加模型配置
    if 'DEEPSEEK_BASE_URL=https://api.deepseek.com' in line and 'DEEPSEEK_MODEL_QUERY_DECOMPOSITION' not in ''.join(new_lines):
        new_lines.append('\n')
        new_lines.append('# DeepSeek 模型配置 (可选，不设置则使用默认值)\n')
        new_lines.append('# 查询分解模型 (默认: deepseek-chat)\n')
        new_lines.append('DEEPSEEK_MODEL_QUERY_DECOMPOSITION=deepseek-chat\n')
        new_lines.append('\n')
        new_lines.append('# 计划生成模型 (默认: deepseek-reasoner，推理模型)\n')
        new_lines.append('DEEPSEEK_MODEL_PLAN_GENERATION=deepseek-reasoner\n')
        new_lines.append('\n')
        new_lines.append('# 答案生成模型 (默认: deepseek-chat)\n')
        new_lines.append('DEEPSEEK_MODEL_ANSWER_GENERATION=deepseek-chat\n')
        new_lines.append('\n')
        new_lines.append('# DeepSeek 超时配置 (可选，如果设置了会覆盖 LLM_TIMEOUT)\n')
        new_lines.append('# DEEPSEEK_TIMEOUT=60.0\n')
    
    # 在 Qwen Base URL 之后添加模型配置
    if 'QWEN_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1' in line and 'QWEN_MODEL_QUERY_DECOMPOSITION' not in ''.join(new_lines):
        new_lines.append('\n')
        new_lines.append('# Qwen 模型配置 (可选，不设置则使用默认值)\n')
        new_lines.append('# 查询分解模型 (默认: qwen-turbo，快速)\n')
        new_lines.append('QWEN_MODEL_QUERY_DECOMPOSITION=qwen-turbo\n')
        new_lines.append('\n')
        new_lines.append('# 计划生成模型 (默认: qwen-max，高质量)\n')
        new_lines.append('QWEN_MODEL_PLAN_GENERATION=qwen-max\n')
        new_lines.append('\n')
        new_lines.append('# 答案生成模型 (默认: qwen-plus，平衡)\n')
        new_lines.append('QWEN_MODEL_ANSWER_GENERATION=qwen-plus\n')
        new_lines.append('\n')
        new_lines.append('# Qwen 超时配置 (可选，如果设置了会覆盖 LLM_TIMEOUT)\n')
        new_lines.append('# QWEN_TIMEOUT=60.0\n')
    
    # 在 Jina Base URL 之后添加超时配置
    if 'JINA_BASE_URL=' in line and 'JINA_TIMEOUT' not in ''.join(new_lines):
        new_lines.append('\n')
        new_lines.append('# Jina 超时配置 (可选，如果设置了会覆盖 LLM_TIMEOUT)\n')
        new_lines.append('# 默认值：30.0 秒\n')
        new_lines.append('JINA_TIMEOUT=30.0\n')
    
    i += 1

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Successfully updated {env_example_path}")
print("\n新增的配置项：")
print("  - LLM_TIMEOUT (通用超时)")
print("  - DEEPSEEK_MODEL_QUERY_DECOMPOSITION")
print("  - DEEPSEEK_MODEL_PLAN_GENERATION")
print("  - DEEPSEEK_MODEL_ANSWER_GENERATION")
print("  - QWEN_MODEL_QUERY_DECOMPOSITION")
print("  - QWEN_MODEL_PLAN_GENERATION")
print("  - QWEN_MODEL_ANSWER_GENERATION")
print("  - JINA_TIMEOUT")

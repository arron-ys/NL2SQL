"""
更新 .env.example 文件，添加 DeepSeek、Qwen 模型配置和通用超时配置
"""
import re
from pathlib import Path

# 获取项目根目录
project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

# 读取现有内容
with open(env_example_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 要添加的通用超时配置（在 LLM 配置部分开头）
llm_timeout_config = """
# ============================================================
# 通用超时配置 (General Timeout Configuration)
# ============================================================
# 通用 LLM 超时配置（所有 LLM provider 都使用，除非设置了 provider 特定超时）
# 默认值：60.0 秒（OpenAI/DeepSeek/Qwen），30.0 秒（Jina）
LLM_TIMEOUT=60.0
"""

# 要添加的 DeepSeek 模型配置（在 DeepSeek 配置部分）
deepseek_model_config = """
# DeepSeek 模型配置 (可选，不设置则使用默认值)
# 查询分解模型 (默认: deepseek-chat)
DEEPSEEK_MODEL_QUERY_DECOMPOSITION=deepseek-chat

# 计划生成模型 (默认: deepseek-reasoner，推理模型)
DEEPSEEK_MODEL_PLAN_GENERATION=deepseek-reasoner

# 答案生成模型 (默认: deepseek-chat)
DEEPSEEK_MODEL_ANSWER_GENERATION=deepseek-chat

# DeepSeek 超时配置 (可选，如果设置了会覆盖 LLM_TIMEOUT)
# DEEPSEEK_TIMEOUT=60.0
"""

# 要添加的 Qwen 模型配置（在 Qwen 配置部分）
qwen_model_config = """
# Qwen 模型配置 (可选，不设置则使用默认值)
# 查询分解模型 (默认: qwen-turbo，快速)
QWEN_MODEL_QUERY_DECOMPOSITION=qwen-turbo

# 计划生成模型 (默认: qwen-max，高质量)
QWEN_MODEL_PLAN_GENERATION=qwen-max

# 答案生成模型 (默认: qwen-plus，平衡)
QWEN_MODEL_ANSWER_GENERATION=qwen-plus

# Qwen 超时配置 (可选，如果设置了会覆盖 LLM_TIMEOUT)
# QWEN_TIMEOUT=60.0
"""

# 要添加的 Jina 超时配置（在 Jina 配置部分）
jina_timeout_config = """
# Jina 超时配置 (可选，如果设置了会覆盖 LLM_TIMEOUT)
# 默认值：30.0 秒
JINA_TIMEOUT=30.0
"""

# 1. 在 LLM 配置部分开头添加通用超时配置
pattern = r'(# ============================================================\n# LLM 配置 \(LLM Configuration\))'
match = re.search(pattern, content)
if match:
    insert_pos = match.end()
    content = content[:insert_pos] + llm_timeout_config + content[insert_pos:]

# 2. 在 OpenAI 超时配置之后，更新注释说明
pattern = r'(OPENAI_TIMEOUT=60\.0)'
if re.search(pattern, content):
    content = re.sub(
        pattern,
        r'# OPENAI_TIMEOUT=60.0  # 可选，如果设置了会覆盖 LLM_TIMEOUT',
        content
    )

# 3. 在 DeepSeek Base URL 之后添加模型配置
pattern = r'(DEEPSEEK_BASE_URL=https://api\.deepseek\.com\n)'
match = re.search(pattern, content)
if match and 'DEEPSEEK_MODEL_QUERY_DECOMPOSITION' not in content:
    insert_pos = match.end()
    content = content[:insert_pos] + deepseek_model_config + content[insert_pos:]

# 4. 在 Qwen Base URL 之后添加模型配置
pattern = r'(QWEN_BASE_URL=https://dashscope\.aliyuncs\.com/compatible-mode/v1\n)'
match = re.search(pattern, content)
if match and 'QWEN_MODEL_QUERY_DECOMPOSITION' not in content:
    insert_pos = match.end()
    content = content[:insert_pos] + qwen_model_config + content[insert_pos:]

# 5. 在 Jina Base URL 之后添加超时配置
pattern = r'(JINA_BASE_URL=.*?\n)'
match = re.search(pattern, content)
if match and 'JINA_TIMEOUT' not in content:
    insert_pos = match.end()
    content = content[:insert_pos] + jina_timeout_config + content[insert_pos:]

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.write(content)

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

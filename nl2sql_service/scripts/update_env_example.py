"""
更新 .env.example 文件，添加模型配置和代理配置
"""
import re
from pathlib import Path

# 获取项目根目录
project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

# 读取现有内容
with open(env_example_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 要添加的配置项
new_configs = """
# OpenAI 模型配置 (可选，不设置则使用默认值)
# 查询分解模型 (默认: gpt-4o-mini)
# 可选值: gpt-4o-mini, gpt-4o, gpt-4-turbo, gpt-3.5-turbo 等
OPENAI_MODEL_QUERY_DECOMPOSITION=gpt-4o-mini

# 计划生成模型 (默认: gpt-4o-mini)
# 可选值: gpt-4o-mini, gpt-4o, gpt-4-turbo, gpt-3.5-turbo 等
OPENAI_MODEL_PLAN_GENERATION=gpt-4o-mini

# 答案生成模型 (默认: gpt-4o-mini)
# 可选值: gpt-4o-mini, gpt-4o, gpt-4-turbo, gpt-3.5-turbo 等
OPENAI_MODEL_ANSWER_GENERATION=gpt-4o-mini

# OpenAI 超时配置 (可选，默认: 60 秒)
OPENAI_TIMEOUT=60.0

# ============================================================
# 代理配置 (Proxy Configuration)
# ============================================================
# OpenAI 代理配置 (推荐，专门用于 OpenAI API)
# 如果在中国大陆访问 OpenAI，需要配置代理
# 示例: http://127.0.0.1:7897 (Clash Verge 默认端口)
# 如果不设置，将尝试使用系统级代理 (HTTP_PROXY/HTTPS_PROXY)
# 如果都不设置，则不使用代理（直接连接，可能失败）
OPENAI_PROXY=http://127.0.0.1:7897

# 系统级代理配置 (可选，向后兼容)
# 如果设置了 OPENAI_PROXY，则优先使用 OPENAI_PROXY
# 如果未设置 OPENAI_PROXY，则使用以下系统级代理配置
HTTP_PROXY=http://127.0.0.1:7897
HTTPS_PROXY=http://127.0.0.1:7897
"""

# 查找插入位置（在 OPENAI_BASE_URL 之后）
pattern = r'(OPENAI_BASE_URL=.*?\n)'
match = re.search(pattern, content)

if match:
    # 在 OPENAI_BASE_URL 之后插入
    insert_pos = match.end()
    new_content = content[:insert_pos] + new_configs + content[insert_pos:]
else:
    # 如果找不到，在 LLM 配置部分末尾添加
    # 查找 "DeepSeek 配置" 之前的位置
    pattern = r'(# ============================================================\n# DeepSeek)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.start()
        new_content = content[:insert_pos] + new_configs + content[insert_pos:]
    else:
        # 如果还是找不到，直接追加
        new_content = content + new_configs

# 检查 Jina 模型配置
jina_model_config = """
# Jina 模型配置 (可选，不设置则使用默认值)
# 嵌入模型 (默认: jina-embeddings-v3)
# 可选值: jina-embeddings-v3, jina-embeddings-v2 等
JINA_MODEL_EMBEDDING=jina-embeddings-v3
"""

# 在 JINA_BASE_URL 之后添加 Jina 模型配置
pattern = r'(JINA_BASE_URL=.*?\n)'
match = re.search(pattern, new_content)
if match and 'JINA_MODEL_EMBEDDING' not in new_content:
    insert_pos = match.end()
    new_content = new_content[:insert_pos] + jina_model_config + new_content[insert_pos:]

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.write(new_content)

print(f"✅ 已更新 {env_example_path}")
print("\n新增的配置项：")
print("  - OPENAI_MODEL_QUERY_DECOMPOSITION")
print("  - OPENAI_MODEL_PLAN_GENERATION")
print("  - OPENAI_MODEL_ANSWER_GENERATION")
print("  - OPENAI_TIMEOUT")
print("  - OPENAI_PROXY")
print("  - HTTP_PROXY / HTTPS_PROXY")
print("  - JINA_MODEL_EMBEDDING")

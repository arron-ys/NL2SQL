"""
显示需要在 .env 文件中添加的配置项
"""
print("=" * 60)
print("需要在 .env 文件中添加的配置项")
print("=" * 60)
print("""
# OpenAI 模型配置 (可选，不设置则使用默认值)
OPENAI_MODEL_QUERY_DECOMPOSITION=gpt-4o-mini
OPENAI_MODEL_PLAN_GENERATION=gpt-4o-mini
OPENAI_MODEL_ANSWER_GENERATION=gpt-4o-mini
OPENAI_TIMEOUT=60.0

# 代理配置 (推荐，专门用于 OpenAI API)
# 如果在中国大陆访问 OpenAI，需要配置代理
OPENAI_PROXY=http://127.0.0.1:7897

# ============================================================
# 代理模式（推荐）
# ============================================================
# PROXY_MODE:
# - explicit (默认): 仅使用 provider 专用代理（OPENAI_PROXY / DEEPSEEK_PROXY / QWEN_PROXY），并强制忽略系统 HTTP_PROXY
# - none: 禁用所有代理（强制直连，忽略系统 HTTP_PROXY）
# - system: 信任系统代理（读取 HTTP_PROXY/HTTPS_PROXY/ALL_PROXY）
PROXY_MODE=explicit
#
# PROXY_STRICT:
# - 0 (默认): 显式代理不可达则自动降级直连（并强制 trust_env=False，避免 env proxy 劫持）
# - 1: 显式代理不可达则直接报错（用于快速定位代理端口/进程问题）
PROXY_STRICT=0

# DeepSeek / Qwen 独立代理（可选，不要用 OPENAI_PROXY 污染）
DEEPSEEK_PROXY=
QWEN_PROXY=

# 系统级代理配置 (可选，向后兼容)
HTTP_PROXY=http://127.0.0.1:7897
HTTPS_PROXY=http://127.0.0.1:7897

# Jina 模型配置 (可选，不设置则使用默认值)
JINA_MODEL_EMBEDDING=jina-embeddings-v3
""")
print("=" * 60)
print("说明：")
print("1. 这些配置项都是可选的，如果不设置会使用默认值")
print("2. 默认 PROXY_MODE=explicit：不会读取 HTTP_PROXY/HTTPS_PROXY（避免被系统代理污染）")
print("3. 如需走系统代理，显式设置 PROXY_MODE=system")
print("4. OPENAI/DEEPSEEK/QWEN 建议分别设置 *_PROXY，避免全局污染")
print("=" * 60)

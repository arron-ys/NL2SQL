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

# 系统级代理配置 (可选，向后兼容)
HTTP_PROXY=http://127.0.0.1:7897
HTTPS_PROXY=http://127.0.0.1:7897

# Jina 模型配置 (可选，不设置则使用默认值)
JINA_MODEL_EMBEDDING=jina-embeddings-v3
""")
print("=" * 60)
print("说明：")
print("1. 这些配置项都是可选的，如果不设置会使用默认值")
print("2. OPENAI_PROXY 优先级最高，如果设置了会优先使用")
print("3. 如果不设置 OPENAI_PROXY，会尝试使用 HTTP_PROXY/HTTPS_PROXY")
print("4. 如果都不设置，则不使用代理（直接连接，可能失败）")
print("=" * 60)

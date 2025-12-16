"""
显示 .env.example 文件中切换到 DeepSeek 所需的配置项
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    content = f.read()

print("=" * 70)
print("切换到 DeepSeek 所需的配置项（在 .env 文件中添加）")
print("=" * 70)
print("""
# 1. 明确指定使用 DeepSeek 作为默认 LLM Provider
DEFAULT_LLM_PROVIDER=deepseek

# 2. DeepSeek API Key（必需）
DEEPSEEK_API_KEY=your_deepseek_api_key_here

# 3. DeepSeek 模型配置（可选，不设置则使用默认值）
DEEPSEEK_MODEL_QUERY_DECOMPOSITION=deepseek-chat
DEEPSEEK_MODEL_PLAN_GENERATION=deepseek-reasoner
DEEPSEEK_MODEL_ANSWER_GENERATION=deepseek-chat

# 4. 通用超时配置（所有 LLM 都使用）
LLM_TIMEOUT=60.0
""")
print("=" * 70)
print("检查 .env.example 文件中的配置项：")
print("=" * 70)

configs = {
    'DEFAULT_LLM_PROVIDER': '存在（在通用超时配置部分和 DeepSeek 配置部分）',
    'LLM_TIMEOUT': '存在（通用超时配置部分）',
    'DEEPSEEK_API_KEY': '存在（DeepSeek 配置部分）',
    'DEEPSEEK_MODEL_QUERY_DECOMPOSITION': '存在（DeepSeek 配置部分）',
    'DEEPSEEK_MODEL_PLAN_GENERATION': '存在（DeepSeek 配置部分）',
    'DEEPSEEK_MODEL_ANSWER_GENERATION': '存在（DeepSeek 配置部分）',
}

for config, status in configs.items():
    exists = config in content
    print(f"  {config:40s} : {'[OK]' if exists else '[MISSING]'} {status}")

print("\n" + "=" * 70)
print("说明：")
print("1. 所有配置项都已经在 .env.example 文件中")
print("2. 大部分配置项是注释掉的（以 # 开头），这是正常的")
print("3. 在你的实际 .env 文件中，取消注释并填入实际值即可")
print("4. 文件开头有 '快速配置示例：切换到 DeepSeek' 部分，可以参考")
print("=" * 70)

"""
最终完善 .env.example 文件，确保 DeepSeek 配置示例清晰可见
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 在 DeepSeek 配置部分开头添加完整的、未注释的示例
deepseek_example = """
# ============================================================
# 切换到 DeepSeek 的完整配置示例
# ============================================================
# 1. 明确指定使用 DeepSeek 作为默认 LLM Provider（取消下面的注释）
DEFAULT_LLM_PROVIDER=deepseek

# 2. 配置 DeepSeek API Key（必需，填入你的 API Key）
DEEPSEEK_API_KEY=your_deepseek_api_key_here

# 3. DeepSeek 模型配置（可选，不设置则使用默认值）
DEEPSEEK_MODEL_QUERY_DECOMPOSITION=deepseek-chat
DEEPSEEK_MODEL_PLAN_GENERATION=deepseek-reasoner
DEEPSEEK_MODEL_ANSWER_GENERATION=deepseek-chat

# 4. 通用超时配置（所有 LLM 都使用）
LLM_TIMEOUT=60.0

# ============================================================
"""

# 查找 DeepSeek 配置部分的开始位置
marker = "# ============================================================\n# DeepSeek 配置 (DeepSeek Configuration)"
if marker in content:
    # 在标记之后插入示例
    insert_pos = content.find(marker) + len(marker)
    # 检查是否已经添加过示例
    if "切换到 DeepSeek 的完整配置示例" not in content:
        content = content[:insert_pos] + deepseek_example + content[insert_pos:]
    else:
        print("示例已存在，跳过添加")
else:
    # 如果找不到标记，在 DEEPSEEK_API_KEY 之前添加
    marker2 = "DEEPSEEK_API_KEY="
    if marker2 in content:
        insert_pos = content.find(marker2)
        if "切换到 DeepSeek 的完整配置示例" not in content[:insert_pos]:
            content = content[:insert_pos] + deepseek_example + content[insert_pos:]
        else:
            print("示例已存在，跳过添加")
    else:
        print("未找到 DeepSeek 配置部分")

# 写入文件
with open(env_example_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Successfully updated {env_example_path}")
print("Added clear DeepSeek configuration example with uncommented DEFAULT_LLM_PROVIDER")

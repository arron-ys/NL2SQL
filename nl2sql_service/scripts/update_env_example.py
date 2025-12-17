"""
更新 nl2sql_service/.env.example（可提交的示例配置文件）。

约束：
- 只更新 .env.example，不读取/不修改 .env
- 必须幂等：重复运行不应产生 diff
"""
from pathlib import Path

# 获取项目根目录
project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, "r", encoding="utf-8") as f:
    content = f.read()

# 统一换行符，避免 Windows CRLF 导致匹配失败
content = content.replace("\r\n", "\n")


def replace_section(text: str, start: str, end: str, body: str) -> str:
    """用 start/end marker 替换区块，不存在则追加到 end 前或末尾。"""
    if start in text and end in text and text.index(start) < text.index(end):
        pre = text.split(start)[0]
        post = text.split(end, 1)[1]
        return pre + start + body + end + post
    return text


def remove_section(text: str, start: str, end: str) -> str:
    """删除 start/end marker 区块（若存在）。"""
    if start in text and end in text and text.index(start) < text.index(end):
        pre = text.split(start)[0]
        post = text.split(end, 1)[1]
        return pre + post
    return text


PROXY_SECTION_START = "# >>> PROXY_CONFIG_SECTION (managed by scripts/update_env_example.py)\n"
PROXY_SECTION_END = "# <<< PROXY_CONFIG_SECTION\n"

proxy_section = (
    PROXY_SECTION_START
    + """# ============================================================
# 代理配置 (Proxy Configuration)
# ============================================================
# 说明：
# - 默认不信任系统代理环境变量（HTTP_PROXY/HTTPS_PROXY/ALL_PROXY），除非显式设置 PROXY_MODE=system
# - OpenAI / DeepSeek / Qwen 分别使用各自的 *_PROXY，避免互相污染
#
# PROXY_MODE:
# - none:    禁用所有代理（强制直连），忽略系统代理 env（trust_env=False）
# - explicit:（默认/推荐）仅使用 provider 专用代理（*_PROXY），忽略系统代理 env（trust_env=False）
# - system:  信任系统代理 env（HTTP_PROXY/HTTPS_PROXY/ALL_PROXY），即 trust_env=True
PROXY_MODE=explicit
#
# PROXY_STRICT:
# - 0（默认）: 显式代理不可达则自动降级直连，并强制 trust_env=False（避免再次被系统代理 env 劫持）
# - 1        : 显式代理不可达则直接报错（用于快速定位端口/进程问题）
PROXY_STRICT=0
#
# Provider 专用代理（仅在 PROXY_MODE=explicit 时使用）
OPENAI_PROXY=http://127.0.0.1:7897
DEEPSEEK_PROXY=
QWEN_PROXY=
#
# 系统级代理（仅在 PROXY_MODE=system 时才会生效）
HTTP_PROXY=
HTTPS_PROXY=
"""
    + PROXY_SECTION_END
)

new_content = content

# 1) 删除旧的 managed 区块（避免重复）
new_content = remove_section(new_content, PROXY_SECTION_START, PROXY_SECTION_END)

# 2) 用统一 proxy_section 替换 legacy 代理段落（放在 OpenAI 配置后，DeepSeek 配置前）
legacy_start = "# ============================================================\n# 代理配置 (Proxy Configuration)\n# ============================================================\n"
# 避免不同编码/乱码导致中文匹配失败，这里用英文 token 定位 DeepSeek 章节
legacy_end_token = "DeepSeek Configuration"
if legacy_start in new_content and legacy_end_token in new_content:
    before = new_content.split(legacy_start, 1)[0]
    # 找到 legacy_start 后第一个包含 DeepSeek Configuration 的位置，保留其前面的分隔符行
    rest = new_content.split(legacy_start, 1)[1]
    idx = rest.find(legacy_end_token)
    if idx != -1:
        # 回退到该行开头（包含 '# '）
        line_start = rest.rfind("\n", 0, idx)
        if line_start == -1:
            line_start = 0
        # 尝试把 “# ============================================================” 也包含进来
        sep_start = rest.rfind("# ============================================================", 0, idx)
        cut = sep_start if sep_start != -1 else line_start
        after = rest[cut:]
        new_content = before.rstrip() + "\n\n" + proxy_section + "\n\n" + after
else:
    # fallback：插到 OpenAI_TIMEOUT 注释之后
    anchor = "# OPENAI_TIMEOUT=60.0"
    if anchor in new_content:
        new_content = new_content.replace(anchor, anchor + "\n\n" + proxy_section, 1)
    else:
        new_content = new_content.rstrip() + "\n\n" + proxy_section

# 修复之前脚本意外去掉注释符的问题（把 " DEFAULT_LLM_PROVIDER" 恢复为注释示例）
new_content = new_content.replace("\n DEFAULT_LLM_PROVIDER=deepseek\n", "\n# DEFAULT_LLM_PROVIDER=deepseek\n")

# 修复 OPENAI_TIMEOUT 注释行被错误拆分的问题（保持为单行）
new_content = new_content.replace(
    "# OPENAI_TIMEOUT=60.0\n  # 可选，如果设置了会覆盖 LLM_TIMEOUT",
    "# OPENAI_TIMEOUT=60.0  # 可选，如果设置了会覆盖 LLM_TIMEOUT",
)
# 删除可能残留的缩进行（避免出现“单独一行的  # 可选...”）
new_content = new_content.replace("\n  # 可选，如果设置了会覆盖 LLM_TIMEOUT", "")

# 如果 OPENAI_TIMEOUT 行被截断为纯值，补回说明（幂等）
new_content = new_content.replace(
    "# OPENAI_TIMEOUT=60.0\n",
    "# OPENAI_TIMEOUT=60.0  # 可选，如果设置了会覆盖 LLM_TIMEOUT\n",
)

# 不在文件末尾重复追加旧的 OPENAI_PROXY_STRICT 说明（已由 PROXY_STRICT 取代）
new_content = new_content.replace(
    "\n\n# 代理严格模式请使用 PROXY_STRICT（见上方 Proxy 配置说明）",
    "",
)

# 折叠多余空行（保证生成结果稳定）
while "\n\n\n" in new_content:
    new_content = new_content.replace("\n\n\n", "\n\n")

with open(env_example_path, "w", encoding="utf-8") as f:
    f.write(new_content.rstrip() + "\n")

print(f"Updated {env_example_path}")

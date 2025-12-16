"""
诊断脚本：检查环境变量和 AI Client 初始化情况
"""
import os
import sys
from pathlib import Path

# 添加项目路径
nl2sql_service_dir = Path(__file__).parent.parent
sys.path.insert(0, str(nl2sql_service_dir))

# 加载 .env 文件
from dotenv import load_dotenv
load_dotenv(dotenv_path=nl2sql_service_dir / ".env")

print("=" * 60)
print("环境变量检查")
print("=" * 60)

# 检查 API keys
openai_key = os.getenv("OPENAI_API_KEY")
jina_key = os.getenv("JINA_API_KEY")

print(f"\nOPENAI_API_KEY:")
print(f"  存在: {bool(openai_key)}")
print(f"  长度: {len(openai_key) if openai_key else 0}")
print(f"  前10个字符: {openai_key[:10] if openai_key else 'N/A'}...")
print(f"  以 'sk-' 开头: {openai_key.startswith('sk-') if openai_key else False}")

print(f"\nJINA_API_KEY:")
print(f"  存在: {bool(jina_key)}")
print(f"  长度: {len(jina_key) if jina_key else 0}")

# 检查代理配置
print(f"\n代理配置:")
print(f"  HTTP_PROXY: {os.getenv('HTTP_PROXY', '未设置')}")
print(f"  HTTPS_PROXY: {os.getenv('HTTPS_PROXY', '未设置')}")
print(f"  http_proxy: {os.getenv('http_proxy', '未设置')}")
print(f"  https_proxy: {os.getenv('https_proxy', '未设置')}")

# 检查其他配置
print(f"\n其他配置:")
print(f"  OPENAI_BASE_URL: {os.getenv('OPENAI_BASE_URL', '未设置（使用默认）')}")
print(f"  OPENAI_TIMEOUT: {os.getenv('OPENAI_TIMEOUT', '未设置（使用默认60秒）')}")

print("\n" + "=" * 60)
print("AI Client 初始化测试")
print("=" * 60)

try:
    from core.ai_client import get_ai_client
    
    print("\n正在初始化 AI Client...")
    ai_client = get_ai_client()
    
    print(f"\n已初始化的 Providers: {list(ai_client._providers.keys())}")
    
    if "openai" not in ai_client._providers:
        print("\n❌ 错误: OpenAI provider 未初始化！")
        print("\n可能的原因:")
        print("  1. OPENAI_API_KEY 在 .env 文件中格式不正确（可能有引号、空格等）")
        print("  2. .env 文件没有被正确加载")
        print("  3. API key 在初始化时为空")
    else:
        print("\n✅ OpenAI provider 初始化成功！")
        
    if "jina" not in ai_client._providers:
        print("\n❌ 错误: Jina provider 未初始化！")
    else:
        print("✅ Jina provider 初始化成功！")
        
except Exception as e:
    print(f"\n❌ 初始化失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)

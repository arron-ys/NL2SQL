"""
显示 .env.example 文件中的关键配置段
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

print("=" * 60)
print("关键配置段（通用超时和默认 Provider）")
print("=" * 60)

# 显示通用超时配置部分（大约第 20-40 行）
for i in range(20, min(45, len(lines))):
    line = lines[i]
    if 'LLM_TIMEOUT' in line or 'DEFAULT_LLM_PROVIDER' in line or '通用超时' in line or '默认 LLM' in line:
        print(f"{i+1:3d}: {line.rstrip()}")

print("\n" + "=" * 60)
print("DeepSeek 配置段")
print("=" * 60)

# 显示 DeepSeek 配置部分
for i, line in enumerate(lines):
    if 'DEEPSEEK' in line.upper():
        start = max(0, i - 1)
        end = min(len(lines), i + 15)
        if i < 120:  # 只显示第一次出现的部分
            for j in range(start, end):
                print(f"{j+1:3d}: {lines[j].rstrip()}")
            break

print("=" * 60)

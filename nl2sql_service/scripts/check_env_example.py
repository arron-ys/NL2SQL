"""
检查 .env.example 文件中的配置项
"""
from pathlib import Path

project_root = Path(__file__).parent.parent
env_example_path = project_root / ".env.example"

with open(env_example_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

print("=" * 60)
print("检查 .env.example 文件中的关键配置项")
print("=" * 60)

keywords = [
    'DEFAULT_LLM_PROVIDER',
    'LLM_TIMEOUT',
    'DEEPSEEK_API_KEY',
    'DEEPSEEK_MODEL_QUERY_DECOMPOSITION',
    'DEEPSEEK_MODEL_PLAN_GENERATION',
    'DEEPSEEK_MODEL_ANSWER_GENERATION'
]

for keyword in keywords:
    found = False
    for i, line in enumerate(lines):
        if keyword in line:
            print(f"\n找到 '{keyword}' 在第 {i+1} 行:")
            print(f"  {line.rstrip()}")
            found = True
            # 显示前后几行上下文
            start = max(0, i - 2)
            end = min(len(lines), i + 3)
            print("  上下文:")
            for j in range(start, end):
                marker = ">>> " if j == i else "    "
                print(f"{marker}{j+1}: {lines[j].rstrip()}")
            break
    if not found:
        print(f"\n未找到 '{keyword}'")

print("\n" + "=" * 60)

"""
测试连接韧性改进

验证 JinaProvider 和 OpenAIProvider 的改进是否正常工作。
"""
import asyncio
import httpx
import sys
import time
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


async def test_metrics_endpoint():
    """测试 /metrics 端点"""
    print("\n=== 测试 /metrics 端点 ===")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get("http://localhost:8000/metrics")
            print(f"状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"指标数据: {data}")
                
                # 检查是否有 jina 的指标
                if "metrics" in data and "jina" in data["metrics"]:
                    jina_metrics = data["metrics"]["jina"]
                    print(f"\nJina 指标:")
                    print(f"  - 总请求数: {jina_metrics.get('requests_total', 0)}")
                    print(f"  - 成功数: {jina_metrics.get('success_total', 0)}")
                    print(f"  - 失败数: {jina_metrics.get('failure_total', 0)}")
                    print(f"  - 重试数: {jina_metrics.get('retry_total', 0)}")
                    print(f"  - 错误率: {jina_metrics.get('error_rate', 0):.4f}")
                    print(f"  - 连续失败: {jina_metrics.get('consecutive_failures', 0)}")
                else:
                    print("⚠️  未找到 Jina 指标（可能服务刚启动，还没有请求）")
            else:
                print(f"❌ 请求失败: {response.text}")
        
        except Exception as e:
            print(f"❌ 错误: {e}")


async def test_health_endpoint():
    """测试 /health 端点"""
    print("\n=== 测试 /health 端点 ===")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get("http://localhost:8000/health")
            print(f"状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"健康状态: {data.get('status')}")
                print(f"Providers: {data.get('providers')}")
                
                # 检查所有 provider 是否健康
                providers = data.get("providers", {})
                all_healthy = all(providers.values())
                if all_healthy:
                    print("✅ 所有 providers 健康")
                else:
                    unhealthy = [name for name, ok in providers.items() if not ok]
                    print(f"⚠️  不健康的 providers: {unhealthy}")
            else:
                print(f"❌ 请求失败: {response.text}")
        
        except Exception as e:
            print(f"❌ 错误: {e}")


async def test_execute_request():
    """发送一个实际的 /nl2sql/execute 请求"""
    print("\n=== 测试 /nl2sql/execute 请求 ===")
    
    request_data = {
        "question": "公司当前有多少名员工？",
        "user_id": "test_user",
        "role_id": "ROLE_CEO",
        "tenant_id": "tenant_001",
        "include_trace": False
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            print(f"发送请求: {request_data['question']}")
            start_time = time.time()
            
            response = await client.post(
                "http://localhost:8000/nl2sql/execute",
                json=request_data
            )
            
            elapsed = time.time() - start_time
            print(f"状态码: {response.status_code}")
            print(f"耗时: {elapsed:.2f} 秒")
            
            if response.status_code == 200:
                data = response.json()
                status_val = data.get("status")
                print(f"业务状态: {status_val}")
                
                if status_val == "SUCCESS":
                    print("✅ 请求成功")
                else:
                    print(f"⚠️  请求失败: {data.get('answer_text', '')[:100]}")
            else:
                print(f"❌ HTTP 错误: {response.text[:200]}")
        
        except Exception as e:
            print(f"❌ 错误: {e}")


async def test_multiple_requests():
    """发送多个请求，观察统计指标变化"""
    print("\n=== 测试多个请求（观察指标变化）===")
    
    request_data = {
        "question": "公司总销售额如何？",
        "user_id": "test_user",
        "role_id": "ROLE_CEO",
        "tenant_id": "tenant_001",
        "include_trace": False
    }
    
    num_requests = 3
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        for i in range(num_requests):
            print(f"\n--- 请求 {i+1}/{num_requests} ---")
            try:
                response = await client.post(
                    "http://localhost:8000/nl2sql/execute",
                    json=request_data
                )
                print(f"状态码: {response.status_code}")
                
                # 间隔 2 秒
                if i < num_requests - 1:
                    await asyncio.sleep(2)
            
            except Exception as e:
                print(f"错误: {e}")
        
        # 查看最终指标
        print("\n--- 最终指标 ---")
        try:
            response = await client.get("http://localhost:8000/metrics")
            if response.status_code == 200:
                data = response.json()
                if "metrics" in data and "jina" in data["metrics"]:
                    jina_metrics = data["metrics"]["jina"]
                    print(f"Jina 总请求数: {jina_metrics.get('requests_total', 0)}")
                    print(f"成功数: {jina_metrics.get('success_total', 0)}")
                    print(f"失败数: {jina_metrics.get('failure_total', 0)}")
                    print(f"重试数: {jina_metrics.get('retry_total', 0)}")
                    print(f"错误率: {jina_metrics.get('error_rate', 0):.4f}")
        except Exception as e:
            print(f"获取指标失败: {e}")


async def main():
    """主函数"""
    print("=" * 60)
    print("连接韧性改进测试脚本")
    print("=" * 60)
    
    print("\n⚠️  注意：")
    print("1. 确保服务已启动：python main.py")
    print("2. 确保配置了 JINA_API_KEY 和其他必要的环境变量")
    print("3. 本脚本会发送实际请求到服务")
    
    # 等待用户确认
    print("\n按 Enter 继续，或 Ctrl+C 取消...")
    try:
        input()
    except KeyboardInterrupt:
        print("\n测试已取消")
        return
    
    # 执行测试
    await test_health_endpoint()
    await asyncio.sleep(1)
    
    await test_metrics_endpoint()
    await asyncio.sleep(1)
    
    await test_execute_request()
    await asyncio.sleep(2)
    
    await test_multiple_requests()
    
    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)
    
    print("\n建议：")
    print("1. 查看服务日志，确认是否有重试/健康检查日志")
    print("2. 等待 2 分钟后再次访问 /health，观察健康检查是否工作")
    print("3. 尝试断网后发送请求，观察是否出现 ALERT 日志")


if __name__ == "__main__":
    asyncio.run(main())

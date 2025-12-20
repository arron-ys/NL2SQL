"""
【简述】
验证 Stage3 在 metrics 为空且 warnings 包含 PERMISSION_DENIED 时优先抛出 PermissionDeniedError 而非 MissingMetricError。

【范围/不测什么】
- 不覆盖完整 Stage3 校验流程；仅验证权限错误优先级逻辑。

【用例概述】
- test_stage3_metrics_empty_with_permission_warning_raises_permission_denied_error:
  -- 验证 metrics 为空且包含权限警告时抛出 PermissionDeniedError
"""

from datetime import date
from types import SimpleNamespace

import pytest

from schemas.plan import PlanIntent, QueryPlan
from schemas.request import RequestContext
from stages.stage3_validation import PermissionDeniedError, validate_and_normalize_plan


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stage3_metrics_empty_with_permission_warning_raises_permission_denied_error():
    """
    【测试目标】
    1. 验证 metrics 为空且包含权限警告时抛出 PermissionDeniedError

    【执行过程】
    1. 构造 AGG intent 的 Plan，metrics 为空
    2. warnings 包含 "[PERMISSION_DENIED]" 开头的消息
    3. 调用 validate_and_normalize_plan
    4. 捕获异常类型

    【预期结果】
    1. 抛出 PermissionDeniedError（而非 MissingMetricError）
    """
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[],
        dimensions=[],
        filters=[],
        warnings=["[PERMISSION_DENIED] Blocked metrics: ['销售额'] (Domain: SALES)"],
    )
    context = RequestContext(
        user_id="u1",
        role_id="ROLE_HR_HEAD",
        tenant_id="t1",
        request_id="test-trace-001",
        current_date=date(2024, 1, 15),
    )

    # Dummy registry: Stage3 reads registry.metadata_map in the MissingMetricError logging,
    # but we should exit earlier for PermissionDeniedError.
    registry = SimpleNamespace(metadata_map={})

    with pytest.raises(PermissionDeniedError):
        await validate_and_normalize_plan(plan=plan, context=context, registry=registry)


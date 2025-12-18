"""
Stage3 PermissionDeniedError triggering (unit).

Goal:
- When intent is AGG/TREND and metrics is empty:
  - If warnings contains a string starting with "[PERMISSION_DENIED]",
    Stage3 should raise PermissionDeniedError instead of MissingMetricError.
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


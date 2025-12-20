"""
【简述】
（用一句话写清楚：验证对象 + 行为/规则 + 边界条件）
示例：验证 /nl2sql/plan 在用户提出聚合问题时能生成正确的 AGG 意图与 metrics 列表。

【范围/不测什么】
- （写 1-2 行明确不覆盖什么）
示例：不覆盖真实数据库执行；仅验证 API 响应结构与关键字段。

【用例概述】
- test_case_1_name:
  -- （一句话：该用例要验证的目标；只写目标，不写步骤）
- test_case_2_name:
  -- （一句话：该用例要验证的目标；只写目标，不写步骤）
"""

import pytest


@pytest.mark.unit
def test_case_1_name():
    """
    【测试目标】
    1. （最多 2 条：用规则/性质描述要证明什么）
    2.

    【执行过程】
    1. （最多 6 步：写关键步骤）
    2.
    3.

    【预期结果】
    1. （最多 5 条：可观察结果清单；每条必须能对应到 assert）
    2.
    """
    assert True


class TestGroupName:
    """
    （可选）分组说明：一句话说明该类按什么维度组织用例，例如“E2E Pipeline smoke tests”。
    """

    @pytest.mark.unit
    def test_case_2_name(self):
        """
        【测试目标】
        1.

        【执行过程】
        1.
        2.

        【预期结果】
        1.
        """
        assert True


EXAMPLE_FULL = r"""
【完整示例参考（纯文本，不会被 pytest 执行）】

（这里粘贴一份完整的“真实测试文件长相”示例，供复制参考。注意：这是纯文本，不要在同一个文件里再写第二份 import / test_ 函数。）

示例：execute 默认时间窗
------------------------------------------------------------
\"\"\"
【简述】
验证 /nl2sql/execute 在用户未提供时间范围时，会应用默认时间窗，并将其下推到 SQL 的 WHERE 时间过滤条件中。

【范围/不测什么】
- 不覆盖真实数据库执行与结果正确性；仅验证“时间窗推断 + SQL 过滤条件生成”。
- 不覆盖多租户隔离（tenant_id）相关逻辑。

【用例概述】
- test_default_time_window_is_applied_to_sql_where:
  -- 用户未给时间范围时，SQL 必须包含默认时间窗的 WHERE 过滤。
- test_user_time_range_overrides_default_window:
  -- 用户显式提供时间范围时，必须覆盖默认时间窗。
- test_missing_time_field_returns_explainable_error_or_degrade:
  -- 语义层缺失 time_field 时，应返回明确错误或降级标志，避免“假成功但必为空”。
\"\"\"

import pytest
from httpx import AsyncClient

@pytest.mark.live
async def test_default_time_window_is_applied_to_sql_where(async_client: AsyncClient):
    \"\"\"
    【测试目标】
    1. ...
    \"\"\"
    ...
------------------------------------------------------------
"""

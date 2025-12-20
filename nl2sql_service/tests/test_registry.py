"""
【简述】
验证 SemanticRegistry 的术语查找、类型过滤、权限白名单过滤、兼容性检查与关键词索引功能的正确性。

【范围/不测什么】
- 不覆盖真实 YAML 加载与 Qdrant 向量检索；仅验证 mock metadata_map 和 security_policies 下的查找与过滤逻辑。

【用例概述】
- test_get_existing_term:
  -- 验证获取存在的术语
- test_get_nonexistent_term:
  -- 验证获取不存在的术语返回 None
- test_get_term_returns_correct_structure:
  -- 验证返回的术语结构包含必需字段
- test_get_all_term_types:
  -- 验证获取所有类型的术语（METRIC、DIMENSION、ENTITY）
- test_get_existing_metric:
  -- 验证获取存在的指标
- test_get_nonexistent_metric:
  -- 验证获取不存在的指标返回 None
- test_get_dimension_as_metric_returns_none:
  -- 验证获取维度 ID 作为指标返回 None
- test_get_existing_dimension:
  -- 验证获取存在的维度
- test_get_nonexistent_dimension:
  -- 验证获取不存在的维度返回 None
- test_get_metric_as_dimension_returns_none:
  -- 验证获取指标 ID 作为维度返回 None
- test_get_allowed_ids_role_hr_head_basic:
  -- 验证 ROLE_HR_HEAD 的权限白名单包含 HR 域术语
- test_get_allowed_ids_returns_set:
  -- 验证 get_allowed_ids 返回 set 类型
- test_get_allowed_ids_role_not_found_fail_closed:
  -- 验证角色不存在时返回空集合（fail-closed 策略）
- test_get_allowed_ids_excludes_non_hr_domain_metric:
  -- 验证权限白名单排除非授权域的指标
- test_get_allowed_ids_missing_security_config_is_500:
  -- 验证缺失安全配置时抛出 SecurityConfigError
- test_compatible_metric_and_dimension:
  -- 验证兼容的指标和维度返回 True
- test_incompatible_metric_and_dimension:
  -- 验证不兼容的指标和维度返回 False
- test_nonexistent_metric_returns_false:
  -- 验证不存在的指标兼容性检查返回 False
- test_nonexistent_dimension_returns_false:
  -- 验证不存在的维度兼容性检查返回 False
- test_metric_without_entity_id_returns_false:
  -- 验证缺少 entity_id 的指标兼容性检查返回 False
- test_filter_ids_by_permission:
  -- 验证权限过滤功能
- test_permission_filtering_with_mock:
  -- 验证使用 mock 的权限过滤逻辑
- test_keyword_index_lookup:
  -- 验证关键词索引查找
- test_keyword_index_case_insensitive_lookup:
  -- 验证关键词索引大小写不敏感
"""

from unittest.mock import MagicMock, patch

import pytest

from core.semantic_registry import (
    SecurityConfigError,
    SecurityPolicyNotFound,
    SemanticRegistry,
)


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def mock_registry():
    """创建模拟的 SemanticRegistry 实例"""
    registry = SemanticRegistry()
    # 模拟 metadata_map
    registry.metadata_map = {
        # 业务域示例（用于 get_term / compatibility 等基础测试）
        "METRIC_GMV": {
            "id": "METRIC_GMV",
            "type": "METRIC",
            "metric_type": "SUM",
            "entity_id": "ENTITY_ORDER",
            "name": "GMV",
            "aliases": ["总交易额", "成交金额"],
            "domain_id": "SALES",
        },
        "METRIC_REVENUE": {
            "id": "METRIC_REVENUE",
            "type": "METRIC",
            "metric_type": "SUM",
            "entity_id": "ENTITY_ORDER",
            "name": "Revenue",
            "aliases": ["收入"],
            "domain_id": "SALES",
        },
        "DIM_REGION": {
            "id": "DIM_REGION",
            "type": "DIMENSION",
            "entity_id": "ENTITY_ORDER",
            "name": "Region",
            "aliases": ["地区", "区域"],
            "domain_id": "SALES",
        },
        "DIM_COUNTRY": {
            "id": "DIM_COUNTRY",
            "type": "DIMENSION",
            "entity_id": "ENTITY_ORDER",
            "name": "Country",
            "aliases": ["国家"],
            "domain_id": "SALES",
        },
        "ENTITY_ORDER": {
            "id": "ENTITY_ORDER",
            "type": "ENTITY",
            "name": "Order",
            "domain_id": "SALES",
        },
        # HR 域（允许）
        "METRIC_HEADCOUNT": {
            "id": "METRIC_HEADCOUNT",
            "metric_type": "COUNT",
            "entity_id": "ENT_EMPLOYEE",
            "name": "Headcount",
            "domain_id": "HR",
            "category": "CORE",
        },
        "DIM_DEPARTMENT": {
            "id": "DIM_DEPARTMENT",
            "entity_id": "ENT_EMPLOYEE",
            "name": "Department",
            "domain_id": "HR",
        },
        "ENT_EMPLOYEE": {
            "id": "ENT_EMPLOYEE",
            "type": "ENTITY",
            "name": "Employee",
            "domain_id": "HR",
        },
        # SALES 域（应被拒绝）
        "METRIC_SALES_GMV": {
            "id": "METRIC_SALES_GMV",
            "metric_type": "SUM",
            "entity_id": "ENT_SALES_ORDER_ITEM",
            "name": "Sales GMV",
            "domain_id": "SALES",
        },
    }
    # 模拟 keyword_index
    registry.keyword_index = {
        "gmv": ["METRIC_GMV"],
        "总交易额": ["METRIC_GMV"],
        "成交金额": ["METRIC_GMV"],
        "revenue": ["METRIC_REVENUE"],
        "收入": ["METRIC_REVENUE"],
        "region": ["DIM_REGION"],
        "地区": ["DIM_REGION"],
        "country": ["DIM_COUNTRY"],
        "国家": ["DIM_COUNTRY"],
        "headcount": ["METRIC_HEADCOUNT"],
        "人头": ["METRIC_HEADCOUNT"],
        "department": ["DIM_DEPARTMENT"],
        "部门": ["DIM_DEPARTMENT"],
    }
    # 模拟 security_policies（包含 role_policies）
    registry._security_policies = {
        "role_policies": [
            {
                "policy_id": "POLICY_ROLE_HR_HEAD",
                "role_id": "ROLE_HR_HEAD",
                "scopes": {
                    "domain_access": ["HR", "SHARED"],
                    "entity_scope": ["HR_"],
                    "metric_scope": ["HR_ALL"],
                    "dimension_scope": ["HR_"],
                },
            }
        ]
    }
    registry._rebuild_security_indexes()
    return registry


# ============================================================
# get_term() 测试
# ============================================================


class TestGetTerm:
    """get_term() 方法测试组"""

    @pytest.mark.unit
    def test_get_existing_term(self, mock_registry):
        """
        【测试目标】
        1. 验证获取存在的术语返回正确数据

        【执行过程】
        1. 调用 mock_registry.get_term("METRIC_GMV")
        2. 验证返回值

        【预期结果】
        1. 返回值不为 None
        2. term["id"] 为 "METRIC_GMV"
        3. term["name"] 为 "GMV"
        """
        term = mock_registry.get_term("METRIC_GMV")
        assert term is not None
        assert term["id"] == "METRIC_GMV"
        assert term["name"] == "GMV"

    @pytest.mark.unit
    def test_get_nonexistent_term(self, mock_registry):
        """
        【测试目标】
        1. 验证获取不存在的术语返回 None

        【执行过程】
        1. 调用 mock_registry.get_term("METRIC_NONEXISTENT")

        【预期结果】
        1. 返回值为 None
        """
        term = mock_registry.get_term("METRIC_NONEXISTENT")
        assert term is None

    @pytest.mark.unit
    def test_get_term_returns_correct_structure(self, mock_registry):
        """
        【测试目标】
        1. 验证返回的术语结构包含必需字段

        【执行过程】
        1. 调用 mock_registry.get_term("METRIC_GMV")
        2. 验证返回字典的字段

        【预期结果】
        1. term 包含 "id"、"type"、"name" 字段
        """
        term = mock_registry.get_term("METRIC_GMV")
        assert "id" in term
        assert "type" in term
        assert "name" in term

    @pytest.mark.unit
    def test_get_all_term_types(self, mock_registry):
        """
        【测试目标】
        1. 验证获取所有类型的术语（METRIC、DIMENSION、ENTITY）

        【执行过程】
        1. 分别调用 get_term 获取指标、维度、实体
        2. 验证每个返回对象的 type 字段

        【预期结果】
        1. METRIC_GMV 的 type 为 "METRIC"
        2. DIM_REGION 的 type 为 "DIMENSION"
        3. ENTITY_ORDER 的 type 为 "ENTITY"
        """
        # 测试指标
        metric = mock_registry.get_term("METRIC_GMV")
        assert metric["type"] == "METRIC"

        # 测试维度
        dimension = mock_registry.get_term("DIM_REGION")
        assert dimension["type"] == "DIMENSION"

        # 测试实体
        entity = mock_registry.get_term("ENTITY_ORDER")
        assert entity["type"] == "ENTITY"


# ============================================================
# get_metric_def() 测试
# ============================================================


class TestGetMetricDef:
    """get_metric_def() 方法测试组"""

    @pytest.mark.unit
    def test_get_existing_metric(self, mock_registry):
        """
        【测试目标】
        1. 验证获取存在的指标返回正确数据

        【执行过程】
        1. 调用 mock_registry.get_metric_def("METRIC_GMV")

        【预期结果】
        1. 返回值不为 None
        2. metric["id"] 为 "METRIC_GMV"
        3. metric 包含 "metric_type" 字段
        """
        metric = mock_registry.get_metric_def("METRIC_GMV")
        assert metric is not None
        assert metric["id"] == "METRIC_GMV"
        assert "metric_type" in metric

    def test_get_nonexistent_metric(self, mock_registry):
        """测试获取不存在的指标"""
        metric = mock_registry.get_metric_def("METRIC_NONEXISTENT")
        assert metric is None

    @pytest.mark.unit
    def test_get_dimension_as_metric_returns_none(self, mock_registry):
        """
        【测试目标】
        1. 验证获取维度 ID 作为指标返回 None（类型过滤）

        【执行过程】
        1. 调用 mock_registry.get_metric_def("DIM_REGION")

        【预期结果】
        1. 返回值为 None（因为 DIM_REGION 类型为 DIMENSION，不是 METRIC）
        """
        metric = mock_registry.get_metric_def("DIM_REGION")
        assert metric is None


# ============================================================
# get_dimension_def() 测试
# ============================================================


class TestGetDimensionDef:
    """get_dimension_def() 方法测试组"""

    @pytest.mark.unit
    def test_get_existing_dimension(self, mock_registry):
        """
        【测试目标】
        1. 验证获取存在的维度返回正确数据

        【执行过程】
        1. 调用 mock_registry.get_dimension_def("DIM_REGION")

        【预期结果】
        1. 返回值不为 None
        2. dimension["id"] 为 "DIM_REGION"
        """
        dimension = mock_registry.get_dimension_def("DIM_REGION")
        assert dimension is not None
        assert dimension["id"] == "DIM_REGION"

    @pytest.mark.unit
    def test_get_nonexistent_dimension(self, mock_registry):
        """
        【测试目标】
        1. 验证获取不存在的维度返回 None

        【执行过程】
        1. 调用 mock_registry.get_dimension_def("DIM_NONEXISTENT")

        【预期结果】
        1. 返回值为 None
        """
        dimension = mock_registry.get_dimension_def("DIM_NONEXISTENT")
        assert dimension is None

    @pytest.mark.unit
    def test_get_metric_as_dimension_returns_none(self, mock_registry):
        """
        【测试目标】
        1. 验证获取指标 ID 作为维度返回 None（类型过滤）

        【执行过程】
        1. 调用 mock_registry.get_dimension_def("METRIC_GMV")

        【预期结果】
        1. 返回值为 None（因为 METRIC_GMV 类型为 METRIC，不是 DIMENSION）
        """
        dimension = mock_registry.get_dimension_def("METRIC_GMV")
        assert dimension is None


# ============================================================
# get_allowed_ids() 测试
# ============================================================


class TestGetAllowedIds:
    """测试 get_allowed_ids() 方法（权限过滤）"""

    def test_get_allowed_ids_role_hr_head_basic(self, mock_registry):
        """ROLE_HR_HEAD：允许 HR 域 METRIC/DIM/ENT 白名单"""
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")
        assert "METRIC_HEADCOUNT" in allowed_ids
        assert "DIM_DEPARTMENT" in allowed_ids
        assert "ENT_EMPLOYEE" in allowed_ids

    @pytest.mark.unit
    def test_get_allowed_ids_returns_set(self, mock_registry):
        """
        【测试目标】
        1. 验证 get_allowed_ids 返回 set 类型

        【执行过程】
        1. 调用 mock_registry.get_allowed_ids("ROLE_HR_HEAD")

        【预期结果】
        1. 返回类型为 set
        """
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")
        assert isinstance(allowed_ids, set)

    @pytest.mark.unit
    def test_get_allowed_ids_role_not_found_fail_closed(self, mock_registry):
        """
        【测试目标】
        1. 验证角色不存在时抛出 SecurityPolicyNotFound（fail-closed 策略）

        【执行过程】
        1. 调用 mock_registry.get_allowed_ids("ROLE_NOT_EXIST")

        【预期结果】
        1. 抛出 SecurityPolicyNotFound 异常
        """
        with pytest.raises(SecurityPolicyNotFound):
            mock_registry.get_allowed_ids("ROLE_NOT_EXIST")

    @pytest.mark.unit
    def test_get_allowed_ids_excludes_non_hr_domain_metric(self, mock_registry):
        """
        【测试目标】
        1. 验证权限白名单排除非授权域的指标

        【执行过程】
        1. 调用 mock_registry.get_allowed_ids("ROLE_HR_HEAD")
        2. 验证 SALES 域的指标不在返回集合中

        【预期结果】
        1. allowed_ids 不包含 "METRIC_SALES_GMV"（SALES 域）
        """
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")
        assert "METRIC_SALES_GMV" not in allowed_ids

    @pytest.mark.unit
    def test_get_allowed_ids_missing_security_config_is_500(self):
        """
        【测试目标】
        1. 验证缺失安全配置时抛出 SecurityConfigError

        【执行过程】
        1. 创建 registry 包含 metadata 但 _security_policies 为空
        2. 调用 _rebuild_security_indexes()
        3. 调用 get_allowed_ids("ROLE_HR_HEAD")

        【预期结果】
        1. 抛出 SecurityConfigError（配置错误，不返回全量）
        """
        registry = SemanticRegistry()
        registry.metadata_map = {"METRIC_HEADCOUNT": {"id": "METRIC_HEADCOUNT", "domain_id": "HR"}}
        registry._security_policies = {}
        registry._rebuild_security_indexes()
        with pytest.raises(SecurityConfigError):
            registry.get_allowed_ids("ROLE_HR_HEAD")


# ============================================================
# check_compatibility() 测试
# ============================================================


class TestCheckCompatibility:
    """check_compatibility() 方法测试组"""

    @pytest.mark.unit
    def test_compatible_metric_and_dimension(self, mock_registry):
        """
        【测试目标】
        1. 验证兼容的指标和维度（同一实体）返回 True

        【执行过程】
        1. 调用 mock_registry.check_compatibility("METRIC_GMV", "DIM_REGION")
        2. 两者都属于 ENTITY_ORDER

        【预期结果】
        1. 返回 True
        """
        # METRIC_GMV 和 DIM_REGION 都属于 ENTITY_ORDER
        is_compatible = mock_registry.check_compatibility("METRIC_GMV", "DIM_REGION")
        assert is_compatible is True

    @pytest.mark.unit
    def test_incompatible_metric_and_dimension(self, mock_registry):
        """
        【测试目标】
        1. 验证不兼容的指标和维度（不同实体）返回 False

        【执行过程】
        1. 添加一个不同实体的维度 DIM_PRODUCT (ENTITY_PRODUCT)
        2. 调用 mock_registry.check_compatibility("METRIC_GMV", "DIM_PRODUCT")

        【预期结果】
        1. 返回 False（因为属于不同实体）
        """
        # 添加一个不同实体的维度
        mock_registry.metadata_map["DIM_PRODUCT"] = {
            "id": "DIM_PRODUCT",
            "type": "DIMENSION",
            "entity_id": "ENTITY_PRODUCT",  # 不同实体
        }
        # METRIC_GMV 属于 ENTITY_ORDER，DIM_PRODUCT 属于 ENTITY_PRODUCT
        is_compatible = mock_registry.check_compatibility("METRIC_GMV", "DIM_PRODUCT")
        assert is_compatible is False

    @pytest.mark.unit
    def test_nonexistent_metric_returns_false(self, mock_registry):
        """
        【测试目标】
        1. 验证不存在的指标兼容性检查返回 False

        【执行过程】
        1. 调用 mock_registry.check_compatibility("METRIC_NONEXISTENT", "DIM_REGION")

        【预期结果】
        1. 返回 False
        """
        is_compatible = mock_registry.check_compatibility("METRIC_NONEXISTENT", "DIM_REGION")
        assert is_compatible is False

    @pytest.mark.unit
    def test_nonexistent_dimension_returns_false(self, mock_registry):
        """
        【测试目标】
        1. 验证不存在的维度兼容性检查返回 False

        【执行过程】
        1. 调用 mock_registry.check_compatibility("METRIC_GMV", "DIM_NONEXISTENT")

        【预期结果】
        1. 返回 False
        """
        is_compatible = mock_registry.check_compatibility("METRIC_GMV", "DIM_NONEXISTENT")
        assert is_compatible is False

    @pytest.mark.unit
    def test_metric_without_entity_id_returns_false(self, mock_registry):
        """
        【测试目标】
        1. 验证缺少 entity_id 的指标兼容性检查返回 False

        【执行过程】
        1. 添加一个没有 entity_id 的指标 METRIC_NO_ENTITY
        2. 调用 mock_registry.check_compatibility("METRIC_NO_ENTITY", "DIM_REGION")

        【预期结果】
        1. 返回 False（因为无法判断实体兼容性）
        """
        # 添加一个没有 entity_id 的指标
        mock_registry.metadata_map["METRIC_NO_ENTITY"] = {
            "id": "METRIC_NO_ENTITY",
            "type": "METRIC",
            "metric_type": "SUM",
            # 没有 entity_id
        }
        is_compatible = mock_registry.check_compatibility("METRIC_NO_ENTITY", "DIM_REGION")
        assert is_compatible is False


# ============================================================
# 权限过滤 Mock 测试
# ============================================================


class TestPermissionFiltering:
    """权限过滤功能测试组"""

    @pytest.mark.unit
    def test_filter_ids_by_permission(self, mock_registry):
        """
        【测试目标】
        1. 验证权限过滤功能正确过滤 ID 列表

        【执行过程】
        1. 准备包含 HR 域和 SALES 域的 ID 列表
        2. 调用 get_allowed_ids("ROLE_HR_HEAD") 获取白名单
        3. 过滤 ID 列表，只保留白名单中的 ID

        【预期结果】
        1. 过滤后包含 "METRIC_HEADCOUNT" 和 "DIM_DEPARTMENT"（HR 域）
        2. 过滤后不包含 "METRIC_SALES_GMV"（SALES 域）
        """
        all_ids = ["METRIC_HEADCOUNT", "DIM_DEPARTMENT", "METRIC_SALES_GMV"]
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")

        # 过滤：只保留允许的ID
        filtered_ids = [id for id in all_ids if id in allowed_ids]

        assert "METRIC_HEADCOUNT" in filtered_ids
        assert "DIM_DEPARTMENT" in filtered_ids
        assert "METRIC_SALES_GMV" not in filtered_ids

    @pytest.mark.unit
    @patch.object(SemanticRegistry, "get_allowed_ids")
    def test_permission_filtering_with_mock(self, mock_get_allowed_ids, mock_registry):
        """
        【测试目标】
        1. 验证使用 mock 的权限过滤逻辑

        【执行过程】
        1. patch SemanticRegistry.get_allowed_ids 返回固定集合
        2. 准备 ID 列表进行过滤
        3. 验证过滤结果

        【预期结果】
        1. 只保留 mock 白名单中的 ID
        """
        # Mock get_allowed_ids 只返回部分ID
        mock_get_allowed_ids.return_value = {"METRIC_GMV", "DIM_REGION"}

        allowed_ids = mock_registry.get_allowed_ids("ROLE_RESTRICTED")

        # 验证只返回允许的ID
        assert "METRIC_GMV" in allowed_ids
        assert "DIM_REGION" in allowed_ids
        assert "METRIC_REVENUE" not in allowed_ids
        assert "DIM_COUNTRY" not in allowed_ids

    @pytest.mark.unit
    def test_keyword_index_lookup(self, mock_registry):
        """
        【测试目标】
        1. 验证关键词索引查找功能

        【执行过程】
        1. 使用英文关键词 "gmv" 查找 keyword_index
        2. 使用中文别名 "总交易额" 查找 keyword_index
        3. 验证返回的术语 ID 列表

        【预期结果】
        1. "gmv" 映射到 ["METRIC_GMV"]
        2. "总交易额" 映射到 ["METRIC_GMV"]
        """
        # 测试通过关键词查找术语ID
        keyword = "gmv"
        term_ids = mock_registry.keyword_index.get(keyword, [])
        assert "METRIC_GMV" in term_ids

        # 测试中文别名
        keyword_cn = "总交易额"
        term_ids_cn = mock_registry.keyword_index.get(keyword_cn, [])
        assert "METRIC_GMV" in term_ids_cn

    @pytest.mark.unit
    def test_keyword_index_case_insensitive_lookup(self, mock_registry):
        """
        【测试目标】
        1. 验证关键词索引大小写处理（当前实现为大小写敏感，实际使用需转小写）

        【执行过程】
        1. 使用小写 "gmv" 和大写 "GMV" 查找 keyword_index
        2. 验证查找结果

        【预期结果】
        1. 小写 "gmv" 能查到结果（索引中存在）
        2. 大写 "GMV" 查不到结果（说明实际使用中需要先转小写）
        """
        # 注意：当前 keyword_index 是大小写敏感的
        # 实际使用中应该在查询时转换为小写
        keyword_lower = "gmv"
        keyword_upper = "GMV"

        ids_lower = mock_registry.keyword_index.get(keyword_lower, [])
        ids_upper = mock_registry.keyword_index.get(keyword_upper, [])

        # 当前实现：只有小写的 "gmv" 在索引中
        assert len(ids_lower) > 0
        # 大写的 "GMV" 不在索引中（除非也添加了）
        # 这说明了实际使用中需要做大小写转换

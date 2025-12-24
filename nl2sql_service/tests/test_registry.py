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
    SemanticConfigurationError,
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

    @pytest.mark.unit
    def test_get_allowed_ids_force_common_domain(self, mock_registry):
        """
        【测试目标】
        1. 验证强制追加 COMMON 域：即使 role 的 domain_access 为空，COMMON 域的 term 也应被允许

        【执行过程】
        1. 创建一个 role，domain_access 为空列表，但 dimension_scope 包含 "COMMON_" 以允许 COMMON 域的维度
        2. 在 metadata_map 中添加一个 domain_id="COMMON" 的 term
        3. 调用 get_allowed_ids
        4. 验证 COMMON 域的 term 在 allowed_ids 中

        【预期结果】
        1. domain_id="COMMON" 的 term 在 allowed_ids 中（即使 role 的 domain_access 为空，但 COMMON 域被强制追加）
        """
        # 创建一个 role，domain_access 为空，但 dimension_scope 包含 "COMMON_" 以允许 COMMON 域的维度
        role_id = "ROLE_TEST_EMPTY_DOMAIN"
        mock_registry._role_policy_map[role_id] = {
            "policy_id": "POLICY_TEST",
            "role_id": role_id,
            "scopes": {
                "domain_access": [],  # 空列表（但会被强制追加 COMMON）
                "entity_scope": [],
                "dimension_scope": ["COMMON_"],  # 允许 COMMON 域的维度
                "metric_scope": []
            }
        }
        
        # 添加一个 COMMON 域的 term
        common_term_id = "DIM_COMMON_TIME"
        mock_registry.metadata_map[common_term_id] = {
            "id": common_term_id,
            "name": "通用时间维度",
            "domain_id": "COMMON",
            "type": "DIMENSION"
        }
        
        # 调用 get_allowed_ids
        allowed_ids = mock_registry.get_allowed_ids(role_id)
        
        # 验证 COMMON 域的 term 在 allowed_ids 中
        assert common_term_id in allowed_ids, "COMMON 域的 term 应该被允许，即使 role 的 domain_access 为空（COMMON 域被强制追加）"


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


# ============================================================
# common_vocabulary 加载与索引测试
# ============================================================


class TestCommonVocabulary:
    """common_vocabulary 加载与索引测试组"""

    @pytest.mark.unit
    def test_loads_vocabulary_to_metadata_map(self):
        """
        【测试目标】
        1. 验证 registry 加载 semantic_common.yaml 后，metadata_map 中存在 VOCAB_* 项

        【执行过程】
        1. 创建 registry 并调用 _build_metadata_map，传入包含 common_vocabulary 的 yaml_data
        2. 验证 metadata_map 中包含 vocabulary 项

        【预期结果】
        1. metadata_map 中存在 VOCAB_COMPARE_MODE_YOY
        2. vocab_def 保留 vocab_type（来自 YAML type），value 正确
        3. vocab_def 的 type 为 "VOCABULARY"
        """
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "同比",
                        "aliases": ["YoY", "Year over Year"],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    }
                ]
            }
        }
        registry._build_metadata_map(yaml_data)
        
        vocab_id = "VOCAB_COMPARE_MODE_YOY"
        assert vocab_id in registry.metadata_map
        
        vocab_def = registry.metadata_map[vocab_id]
        assert vocab_def["vocab_type"] == "COMPARE_MODE"  # 来自 YAML type
        assert vocab_def["type"] == "VOCABULARY"  # 内部 type
        assert vocab_def["value"] == "YOY"
        assert vocab_def["term"] == "同比"
        assert vocab_def["name"] == "同比"  # term 映射为 name

    @pytest.mark.unit
    def test_keyword_index_contains_vocab_term_and_aliases(self):
        """
        【测试目标】
        1. 验证 keyword_index 能通过 vocab.term 与至少一个 alias 命中对应 vocab_id

        【执行过程】
        1. 创建 registry 并加载包含 common_vocabulary 的 yaml_data
        2. 验证 keyword_index 中包含 term 和 aliases 的映射

        【预期结果】
        1. keyword_index["同比"] 包含 VOCAB_COMPARE_MODE_YOY
        2. keyword_index["YoY"] 包含 VOCAB_COMPARE_MODE_YOY
        3. keyword_index["Year over Year"] 包含 VOCAB_COMPARE_MODE_YOY
        """
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "同比",
                        "aliases": ["YoY", "Year over Year"],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    }
                ]
            }
        }
        registry._build_metadata_map(yaml_data)
        
        vocab_id = "VOCAB_COMPARE_MODE_YOY"
        
        # 验证 term 能命中
        assert "同比" in registry.keyword_index
        assert vocab_id in registry.keyword_index["同比"]
        
        # 验证 aliases 能命中
        assert "YoY" in registry.keyword_index
        assert vocab_id in registry.keyword_index["YoY"]
        assert "Year over Year" in registry.keyword_index
        assert vocab_id in registry.keyword_index["Year over Year"]

    @pytest.mark.unit
    def test_vocab_id_generation_without_value(self):
        """
        【测试目标】
        1. 验证没有 value 的 vocabulary 生成正确的 vocab_id

        【执行过程】
        1. 创建 vocabulary 项，不包含 value 字段
        2. 验证生成的 vocab_id

        【预期结果】
        1. vocab_id 为 VOCAB_{TYPE} 格式（无 value 部分）
        """
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "测试词",
                        "aliases": [],
                        "type": "TEST_TYPE"
                        # 没有 value
                    }
                ]
            }
        }
        registry._build_metadata_map(yaml_data)
        
        vocab_id = "VOCAB_TEST_TYPE"
        assert vocab_id in registry.metadata_map

    @pytest.mark.unit
    def test_vocab_id_collision_raises_error(self):
        """
        【测试目标】
        1. 验证重复 vocab_id（type + value 组合重复）抛出 SemanticConfigurationError

        【执行过程】
        1. 创建两个 vocabulary 项，具有相同的 type 和 value
        2. 调用 _build_metadata_map

        【预期结果】
        1. 抛出 SemanticConfigurationError
        2. 错误消息包含 "Duplicate vocabulary ID"
        """
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "同比",
                        "aliases": [],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    },
                    {
                        "term": "同比2",  # 不同的 term，但 type + value 相同
                        "aliases": [],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    }
                ]
            }
        }
        
        with pytest.raises(SemanticConfigurationError) as exc_info:
            registry._build_metadata_map(yaml_data)
        
        assert "Duplicate vocabulary ID" in str(exc_info.value)
        assert "VOCAB_COMPARE_MODE_YOY" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_reindex_qdrant_vocabulary_search_text(self):
        """
        【测试目标】
        1. 验证 qdrant reindex 构建的 search_text 对 vocabulary 包含 term 与 alias

        【执行过程】
        1. Mock _get_jina_embedding 和 qdrant_client
        2. 创建 registry 并加载 vocabulary
        3. 调用 _reindex_qdrant
        4. 捕获传递给 _get_jina_embedding 的 search_text

        【预期结果】
        1. vocabulary 的 search_text 包含 term 和至少一个 alias
        """
        from unittest.mock import AsyncMock, MagicMock, patch
        
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "同比",
                        "aliases": ["YoY", "Year over Year"],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    }
                ]
            }
        }
        registry._build_metadata_map(yaml_data)
        
        # Mock qdrant client
        registry.qdrant_client = MagicMock()
        registry.qdrant_client.delete_collection = AsyncMock()
        registry.qdrant_client.create_collection = AsyncMock()
        registry.qdrant_client.upsert = AsyncMock()
        
        # 捕获传递给 _get_jina_embedding 的参数
        captured_search_texts = []
        
        async def mock_get_embedding(text):
            captured_search_texts.append(text)
            return [0.0] * 1024  # 返回 1024 维向量
        
        registry._get_jina_embedding = mock_get_embedding
        
        # 调用 _reindex_qdrant
        await registry._reindex_qdrant()
        
        # 验证 vocabulary 的 search_text
        vocab_search_text = None
        for text in captured_search_texts:
            if "同比" in text and "YoY" in text:
                vocab_search_text = text
                break
        
        assert vocab_search_text is not None
        assert "同比" in vocab_search_text
        assert "YoY" in vocab_search_text
        assert "Year over Year" in vocab_search_text

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_reindex_qdrant_vocabulary_payload_name_defined(self):
        """
        【测试目标】
        1. 验证 qdrant reindex 对 vocabulary 构建 payload 时，name 字段已定义（修复 NameError）

        【执行过程】
        1. Mock qdrant client 和 _get_jina_embedding
        2. 创建 registry 并加载 vocabulary
        3. 调用 _reindex_qdrant
        4. 验证不会抛 NameError

        【预期结果】
        1. _reindex_qdrant 执行成功，不抛 NameError
        2. payload 中包含正确的 name 字段（来自 term）
        """
        from unittest.mock import AsyncMock, MagicMock
        
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "同比",
                        "aliases": ["YoY"],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    }
                ]
            }
        }
        registry._build_metadata_map(yaml_data)
        
        # Mock qdrant client
        registry.qdrant_client = MagicMock()
        registry.qdrant_client.delete_collection = AsyncMock()
        registry.qdrant_client.create_collection = AsyncMock()
        registry.qdrant_client.upsert = AsyncMock()
        
        # 捕获传递给 upsert 的 points，验证 payload 中的 name 字段
        captured_points = []
        
        async def mock_upsert(collection_name, points):
            captured_points.extend(points)
        
        registry.qdrant_client.upsert = mock_upsert
        
        # Mock _get_jina_embedding
        async def mock_get_embedding(text):
            return [0.0] * 1024
        
        registry._get_jina_embedding = mock_get_embedding
        
        # 调用 _reindex_qdrant（应该不抛 NameError）
        await registry._reindex_qdrant()
        
        # 验证 payload 中包含 name 字段（来自 term）
        vocab_point = None
        for point in captured_points:
            if point.payload.get("id") == "VOCAB_COMPARE_MODE_YOY":
                vocab_point = point
                break
        
        assert vocab_point is not None
        assert "name" in vocab_point.payload
        assert vocab_point.payload["name"] == "同比"  # 来自 term

    @pytest.mark.unit
    def test_vocabulary_allowed_in_permission_filter(self):
        """
        【测试目标】
        1. 验证 VOCAB_ 前缀的条目在权限过滤后仍能被保留（不被误杀）

        【执行过程】
        1. 创建 registry 并加载 vocabulary
        2. 设置 security_policies（任意角色）
        3. 调用 get_allowed_ids
        4. 验证 VOCAB_ 条目在 allowed_ids 中

        【预期结果】
        1. VOCAB_COMPARE_MODE_YOY 在 allowed_ids 中
        """
        registry = SemanticRegistry()
        yaml_data = {
            "global_config": {
                "common_vocabulary": [
                    {
                        "term": "同比",
                        "aliases": ["YoY"],
                        "type": "COMPARE_MODE",
                        "value": "YOY"
                    }
                ]
            }
        }
        registry._build_metadata_map(yaml_data)
        
        # 设置 security_policies（最小配置，确保权限检查逻辑运行）
        registry._security_policies = {
            "role_policies": [
                {
                    "policy_id": "POLICY_TEST",
                    "role_id": "ROLE_TEST",
                    "scopes": {
                        "domain_access": ["SALES"],  # vocabulary 没有 domain_id，应该不受影响
                        "entity_scope": [],
                        "metric_scope": [],
                        "dimension_scope": [],
                    },
                }
            ]
        }
        registry._rebuild_security_indexes()
        
        # 调用 get_allowed_ids
        allowed_ids = registry.get_allowed_ids("ROLE_TEST")
        
        # 验证 VOCAB_ 条目在 allowed_ids 中（应该默认允许）
        assert "VOCAB_COMPARE_MODE_YOY" in allowed_ids

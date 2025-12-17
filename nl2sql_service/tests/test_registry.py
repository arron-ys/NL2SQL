"""
Semantic Registry Test Suite

测试术语查找、权限过滤（Mock测试）。
重点测试：
- get_term() 正确性
- get_allowed_ids() 逻辑（RBAC 白名单过滤，fail-closed）
- 权限过滤功能
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
    """测试 get_term() 方法"""

    def test_get_existing_term(self, mock_registry):
        """测试获取存在的术语"""
        term = mock_registry.get_term("METRIC_GMV")
        assert term is not None
        assert term["id"] == "METRIC_GMV"
        assert term["name"] == "GMV"

    def test_get_nonexistent_term(self, mock_registry):
        """测试获取不存在的术语"""
        term = mock_registry.get_term("METRIC_NONEXISTENT")
        assert term is None

    def test_get_term_returns_correct_structure(self, mock_registry):
        """测试返回的术语结构正确"""
        term = mock_registry.get_term("METRIC_GMV")
        assert "id" in term
        assert "type" in term
        assert "name" in term

    def test_get_all_term_types(self, mock_registry):
        """测试获取所有类型的术语"""
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
    """测试 get_metric_def() 方法"""

    def test_get_existing_metric(self, mock_registry):
        """测试获取存在的指标"""
        metric = mock_registry.get_metric_def("METRIC_GMV")
        assert metric is not None
        assert metric["id"] == "METRIC_GMV"
        assert "metric_type" in metric

    def test_get_nonexistent_metric(self, mock_registry):
        """测试获取不存在的指标"""
        metric = mock_registry.get_metric_def("METRIC_NONEXISTENT")
        assert metric is None

    def test_get_dimension_as_metric_returns_none(self, mock_registry):
        """测试将维度作为指标获取时返回 None"""
        metric = mock_registry.get_metric_def("DIM_REGION")
        assert metric is None


# ============================================================
# get_dimension_def() 测试
# ============================================================


class TestGetDimensionDef:
    """测试 get_dimension_def() 方法"""

    def test_get_existing_dimension(self, mock_registry):
        """测试获取存在的维度"""
        dimension = mock_registry.get_dimension_def("DIM_REGION")
        assert dimension is not None
        assert dimension["id"] == "DIM_REGION"

    def test_get_nonexistent_dimension(self, mock_registry):
        """测试获取不存在的维度"""
        dimension = mock_registry.get_dimension_def("DIM_NONEXISTENT")
        assert dimension is None

    def test_get_metric_as_dimension_returns_none(self, mock_registry):
        """测试将指标作为维度获取时返回 None"""
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

    def test_get_allowed_ids_returns_set(self, mock_registry):
        """测试返回类型为 Set"""
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")
        assert isinstance(allowed_ids, set)

    def test_get_allowed_ids_role_not_found_fail_closed(self, mock_registry):
        """role_id 未配置 policy：fail-closed 直接抛 403 映射用异常"""
        with pytest.raises(SecurityPolicyNotFound):
            mock_registry.get_allowed_ids("ROLE_NOT_EXIST")

    def test_get_allowed_ids_excludes_non_hr_domain_metric(self, mock_registry):
        """ROLE_HR_HEAD：不应包含非 HR 域指标"""
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")
        assert "METRIC_SALES_GMV" not in allowed_ids

    def test_get_allowed_ids_missing_security_config_is_500(self):
        """security 配置缺失：应视为配置错误（500），而不是返回全量"""
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
    """测试 check_compatibility() 方法"""

    def test_compatible_metric_and_dimension(self, mock_registry):
        """测试兼容的指标和维度（同一实体）"""
        # METRIC_GMV 和 DIM_REGION 都属于 ENTITY_ORDER
        is_compatible = mock_registry.check_compatibility("METRIC_GMV", "DIM_REGION")
        assert is_compatible is True

    def test_incompatible_metric_and_dimension(self, mock_registry):
        """测试不兼容的指标和维度（不同实体）"""
        # 添加一个不同实体的维度
        mock_registry.metadata_map["DIM_PRODUCT"] = {
            "id": "DIM_PRODUCT",
            "type": "DIMENSION",
            "entity_id": "ENTITY_PRODUCT",  # 不同实体
        }
        # METRIC_GMV 属于 ENTITY_ORDER，DIM_PRODUCT 属于 ENTITY_PRODUCT
        is_compatible = mock_registry.check_compatibility("METRIC_GMV", "DIM_PRODUCT")
        assert is_compatible is False

    def test_nonexistent_metric_returns_false(self, mock_registry):
        """测试不存在的指标返回 False"""
        is_compatible = mock_registry.check_compatibility("METRIC_NONEXISTENT", "DIM_REGION")
        assert is_compatible is False

    def test_nonexistent_dimension_returns_false(self, mock_registry):
        """测试不存在的维度返回 False"""
        is_compatible = mock_registry.check_compatibility("METRIC_GMV", "DIM_NONEXISTENT")
        assert is_compatible is False

    def test_metric_without_entity_id_returns_false(self, mock_registry):
        """测试没有 entity_id 的指标返回 False"""
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
    """测试权限过滤功能（Mock）"""

    def test_filter_ids_by_permission(self, mock_registry):
        """测试根据权限过滤ID列表"""
        all_ids = ["METRIC_HEADCOUNT", "DIM_DEPARTMENT", "METRIC_SALES_GMV"]
        allowed_ids = mock_registry.get_allowed_ids("ROLE_HR_HEAD")

        # 过滤：只保留允许的ID
        filtered_ids = [id for id in all_ids if id in allowed_ids]

        assert "METRIC_HEADCOUNT" in filtered_ids
        assert "DIM_DEPARTMENT" in filtered_ids
        assert "METRIC_SALES_GMV" not in filtered_ids

    @patch.object(SemanticRegistry, "get_allowed_ids")
    def test_permission_filtering_with_mock(self, mock_get_allowed_ids, mock_registry):
        """测试使用Mock模拟权限过滤"""
        # Mock get_allowed_ids 只返回部分ID
        mock_get_allowed_ids.return_value = {"METRIC_GMV", "DIM_REGION"}

        allowed_ids = mock_registry.get_allowed_ids("ROLE_RESTRICTED")

        # 验证只返回允许的ID
        assert "METRIC_GMV" in allowed_ids
        assert "DIM_REGION" in allowed_ids
        assert "METRIC_REVENUE" not in allowed_ids
        assert "DIM_COUNTRY" not in allowed_ids

    def test_keyword_index_lookup(self, mock_registry):
        """测试关键词索引查找"""
        # 测试通过关键词查找术语ID
        keyword = "gmv"
        term_ids = mock_registry.keyword_index.get(keyword, [])
        assert "METRIC_GMV" in term_ids

        # 测试中文别名
        keyword_cn = "总交易额"
        term_ids_cn = mock_registry.keyword_index.get(keyword_cn, [])
        assert "METRIC_GMV" in term_ids_cn

    def test_keyword_index_case_insensitive_lookup(self, mock_registry):
        """测试关键词索引大小写不敏感查找（需要在实际使用中处理）"""
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

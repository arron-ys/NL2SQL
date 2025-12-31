"""
【简述�?
验证 RLS 策略获取功能：索引重建不会丢�?fragments/bindings，SELF/DEPT/COMPANY scope 正确生成 RLS SQL，fail-closed 机制正确�?

【范�?不测什么�?
- 不覆盖真实数据库执行；仅验证 RLS SQL 生成逻辑与索引重建的正确性�?

【用例概述�?
- test_rebuild_security_indexes_does_not_lose_fragments_when_get_allowed_ids_called:
  -- 验证 get_allowed_ids 调用 _rebuild_security_indexes 时使�?yaml_data_snapshot，不会丢�?fragments/bindings
- test_get_rls_policies_self_scope_ok:
  -- 验证 SELF scope 正确生成 RLS SQL（sales_rep_employee_number = user_id�?
- test_get_rls_policies_dept_scope_ok:
  -- 验证 DEPT scope 正确生成 RLS SQL（包�?dim_org_scope 子查询和 tenant_id 过滤�?
- test_get_rls_policies_company_scope_ok:
  -- 验证 COMPANY scope 返回�?RLS SQL 列表
- test_get_rls_policies_role_not_found_fail_closed:
  -- 验证 role_id 不存在时抛出 SecurityPolicyNotFound（fail-closed�?
"""

import pytest

from core.semantic_registry import SemanticRegistry, SecurityPolicyNotFound, SecurityConfigError


class TestRLSPoliciesIndexRebuild:
    """RLS 索引重建测试�?""

    @pytest.mark.unit
    def test_rebuild_security_indexes_does_not_lose_fragments_when_get_allowed_ids_called(self):
        """
        【测试目标�?
        1. 验证 get_allowed_ids 调用 _rebuild_security_indexes 时使�?yaml_data_snapshot，不会丢�?fragments/bindings

        【执行过程�?
        1. 创建 SemanticRegistry 实例
        2. 准备包含 policy_fragments �?row_scope_bindings �?yaml_data
        3. 调用 _build_metadata_map(yaml_data) 初始化索�?
        4. 清空 _role_policy_map 模拟索引丢失
        5. 调用 get_allowed_ids，验证会使用 snapshot 重建索引
        6. 验证 _policy_fragments_map �?_row_scope_binding_map 未被清空

        【预期结果�?
        1. get_allowed_ids 调用成功，不抛出 SecurityConfigError
        2. _policy_fragments_map 包含预期�?fragment
        3. _row_scope_binding_map 包含预期�?binding
        """
        registry = SemanticRegistry()
        
        # 准备 yaml_data（包�?security, policy_fragments, row_scope_bindings�?
        yaml_data = {
            "security": {
                "role_policies": [
                    {
                        "policy_id": "POLICY_TEST",
                        "role_id": "ROLE_TEST",
                        "scopes": {
                            "row_scope_code": "SELF",
                            "domain_access": ["SALES"]
                        }
                    }
                ]
            },
            "policy_fragments": [
                {
                    "fragment_id": "FRAG_SALES_SELF_ORDER_RLS",
                    "type": "ROW_LEVEL",
                    "domain_id": "SALES",
                    "entity_id": "ENT_SALES_ORDER_ITEM",
                    "raw_condition": "sales_rep_employee_number = {{ current_user.employee_id }}"
                }
            ],
            "row_scope_bindings": [
                {
                    "row_scope_code": "SELF",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": "FRAG_SALES_SELF_ORDER_RLS"
                        }
                    ]
                }
            ]
        }
        
        # 初始化索�?
        registry._build_metadata_map(yaml_data)
        
        # 验证索引已建�?
        assert "FRAG_SALES_SELF_ORDER_RLS" in registry._policy_fragments_map
        assert ("SELF", "SALES", "ENT_SALES_ORDER_ITEM") in registry._row_scope_binding_map
        
        # 清空 _role_policy_map 模拟索引丢失
        registry._role_policy_map.clear()
        
        # 调用 get_allowed_ids，应该使�?snapshot 重建索引
        allowed_ids = registry.get_allowed_ids("ROLE_TEST")
        
        # 验证 fragments �?bindings 未被清空
        assert "FRAG_SALES_SELF_ORDER_RLS" in registry._policy_fragments_map
        assert ("SELF", "SALES", "ENT_SALES_ORDER_ITEM") in registry._row_scope_binding_map
        assert registry._role_policy_map["ROLE_TEST"] is not None


class TestRLSPoliciesGeneration:
    """RLS SQL 生成测试�?""

    @pytest.mark.unit
    def test_get_rls_policies_self_scope_ok(self):
        """
        【测试目标�?
        1. 验证 SELF scope 正确生成 RLS SQL（sales_rep_employee_number = user_id�?

        【执行过程�?
        1. 创建 SemanticRegistry 实例
        2. 准备包含 SELF scope 配置�?yaml_data
        3. 调用 _build_metadata_map 初始�?
        4. 调用 get_rls_policies 获取 RLS SQL
        5. 验证返回�?SQL 包含正确的过滤条�?

        【预期结果�?
        1. 返回�?SQL 列表长度�?1
        2. SQL 包含 "sales_rep_employee_number = 1001"（user_id=1001�?
        3. SQL 不包含占位符 {{ }}
        """
        registry = SemanticRegistry()
        
        yaml_data = {
            "security": {
                "role_policies": [
                    {
                        "policy_id": "POLICY_ROLE_SALES_STAFF",
                        "role_id": "ROLE_SALES_STAFF",
                        "scopes": {
                            "row_scope_code": "SELF",
                            "domain_access": ["SALES"]
                        }
                    }
                ]
            },
            "policy_fragments": [
                {
                    "fragment_id": "FRAG_SALES_SELF_ORDER_RLS",
                    "type": "ROW_LEVEL",
                    "domain_id": "SALES",
                    "entity_id": "ENT_SALES_ORDER_ITEM",
                    "raw_condition": "sales_rep_employee_number = {{ current_user.employee_id }}"
                }
            ],
            "row_scope_bindings": [
                {
                    "row_scope_code": "SELF",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": "FRAG_SALES_SELF_ORDER_RLS"
                        }
                    ]
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 调用 get_rls_policies
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_SALES_STAFF",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        # 验证结果
        assert len(rls_sql_list) == 1
        assert "sales_rep_employee_number = 1001" in rls_sql_list[0]
        assert "{{" not in rls_sql_list[0]
        assert "}}" not in rls_sql_list[0]

    @pytest.mark.unit
    def test_get_rls_policies_dept_scope_ok(self):
        """
        【测试目标�?
        1. 验证 DEPT scope 正确生成 RLS SQL（包�?dim_org_scope 子查询和 tenant_id 过滤�?

        【执行过程�?
        1. 创建 SemanticRegistry 实例
        2. 准备包含 DEPT scope 配置�?yaml_data
        3. 调用 _build_metadata_map 初始�?
        4. 调用 get_rls_policies 获取 RLS SQL
        5. 验证返回�?SQL 包含 dim_org_scope 子查询和 tenant_id 过滤

        【预期结果�?
        1. 返回�?SQL 列表长度�?1
        2. SQL 包含 "FROM dim_org_scope WHERE manager_id = 1001"
        3. SQL 包含 "AND tenant_id = 'tenant_001'"（tenant_id 过滤�?
        4. SQL 不包含占位符 {{ }}
        """
        registry = SemanticRegistry()
        
        yaml_data = {
            "security": {
                "role_policies": [
                    {
                        "policy_id": "POLICY_ROLE_SALES_HEAD",
                        "role_id": "ROLE_SALES_HEAD",
                        "scopes": {
                            "row_scope_code": "DEPT",
                            "domain_access": ["SALES"]
                        }
                    }
                ]
            },
            "policy_fragments": [
                {
                    "fragment_id": "FRAG_SALES_DEPT_ORDER_RLS",
                    "type": "ROW_LEVEL",
                    "domain_id": "SALES",
                    "entity_id": "ENT_SALES_ORDER_ITEM",
                    "raw_condition": "sales_rep_employee_number IN (SELECT e.employee_id FROM v_employee_profile e WHERE e.department_id IN (SELECT dept_id FROM dim_org_scope WHERE manager_id = {{ current_user.employee_id }} AND tenant_id = {{ current_user.tenant_id }}))"
                }
            ],
            "row_scope_bindings": [
                {
                    "row_scope_code": "DEPT",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": "FRAG_SALES_DEPT_ORDER_RLS"
                        }
                    ]
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 调用 get_rls_policies
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_SALES_HEAD",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        # 验证结果
        assert len(rls_sql_list) == 1
        sql = rls_sql_list[0]
        assert "FROM dim_org_scope WHERE manager_id = 1001" in sql
        assert "AND tenant_id = 'tenant_001'" in sql
        assert "{{" not in sql
        assert "}}" not in sql

    @pytest.mark.unit
    def test_get_rls_policies_company_scope_ok(self):
        """
        【测试目标�?
        1. 验证 COMPANY scope 返回�?RLS SQL 列表

        【执行过程�?
        1. 创建 SemanticRegistry 实例
        2. 准备包含 COMPANY scope 配置�?yaml_data
        3. 调用 _build_metadata_map 初始�?
        4. 调用 get_rls_policies 获取 RLS SQL
        5. 验证返回空列�?

        【预期结果�?
        1. 返回�?SQL 列表长度�?0（空列表�?
        """
        registry = SemanticRegistry()
        
        yaml_data = {
            "security": {
                "role_policies": [
                    {
                        "policy_id": "POLICY_ROLE_CEO",
                        "role_id": "ROLE_CEO",
                        "scopes": {
                            "row_scope_code": "COMPANY",
                            "domain_access": ["ALL"]
                        }
                    }
                ]
            },
            "row_scope_bindings": [
                {
                    "row_scope_code": "COMPANY",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": None
                        }
                    ]
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 调用 get_rls_policies
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_CEO",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        # 验证结果（COMPANY scope 返回空列表）
        assert len(rls_sql_list) == 0

    @pytest.mark.unit
    def test_get_rls_policies_dept_scope_selects_correct_domain_binding(self):
        """
        【测试目标�?
        1. 验证�?entity_id 但不�?domain_id �?binding 存在时，选择正确�?domain 绑定
        
        【执行过程�?
        1. 构造两�?binding�?'DEPT','SALES','ENT_SALES_ORDER_ITEM')->frag_sales, ('DEPT','HR','ENT_SALES_ORDER_ITEM')->frag_hr
        2. entity_def.domain_id='SALES'
        3. 调用 get_rls_policies
        4. 验证选择的是 SALES domain �?fragment
        
        【预期结果�?
        1. 返回 SALES domain �?fragment 渲染结果
        2. 包含 SALES 特定�?tenant_id 过滤
        """
        registry = SemanticRegistry()
        
        yaml_data = {
            "security": {
                "role_policies": [
                    {
                        "policy_id": "POLICY_ROLE_SALES_HEAD",
                        "role_id": "ROLE_SALES_HEAD",
                        "scopes": {
                            "row_scope_code": "DEPT",
                            "domain_access": ["SALES"]
                        }
                    }
                ]
            },
            "policy_fragments": [
                {
                    "fragment_id": "FRAG_SALES_DEPT_ORDER_RLS",
                    "type": "ROW_LEVEL",
                    "domain_id": "SALES",
                    "entity_id": "ENT_SALES_ORDER_ITEM",
                    "raw_condition": "sales_rep_employee_number IN (SELECT e.employee_id FROM v_employee_profile e WHERE e.department_id IN (SELECT dept_id FROM dim_org_scope WHERE manager_id = {{ current_user.employee_id }} AND tenant_id = {{ current_user.tenant_id }}))"
                },
                {
                    "fragment_id": "FRAG_HR_DEPT_ORDER_RLS",  # 模拟错误配置：HR domain 绑定�?SALES 实体
                    "type": "ROW_LEVEL",
                    "domain_id": "HR",
                    "entity_id": "ENT_SALES_ORDER_ITEM",
                    "raw_condition": "employee_id IN (SELECT emp_id FROM hr_table WHERE manager_id = {{ current_user.employee_id }})"
                }
            ],
            "row_scope_bindings": [
                {
                    "row_scope_code": "DEPT",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": "FRAG_SALES_DEPT_ORDER_RLS"
                        },
                        {
                            "domain_id": "HR",  # 错误配置：HR 也绑定到 SALES 实体
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": "FRAG_HR_DEPT_ORDER_RLS"
                        }
                    ]
                }
            ],
            "entities": [
                {
                    "id": "ENT_SALES_ORDER_ITEM",
                    "name": "销售订单明�?,
                    "domain_id": "SALES"
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 调用 get_rls_policies
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_SALES_HEAD",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        # 验证结果：应该选择 SALES domain �?fragment
        assert len(rls_sql_list) == 1
        sql = rls_sql_list[0]
        assert "FROM dim_org_scope WHERE manager_id = 1001" in sql
        assert "AND tenant_id = 'tenant_001'" in sql
        assert "sales_rep_employee_number" in sql  # SALES fragment 特有
        assert "hr_table" not in sql  # HR fragment 不应被选中

    @pytest.mark.unit
    def test_get_rls_policies_role_not_found_fail_closed(self):
        """
        【测试目标�?
        1. 验证 role_id 不存在时抛出 SecurityPolicyNotFound（fail-closed�?

        【执行过程�?
        1. 创建 SemanticRegistry 实例
        2. 准备包含其他 role �?yaml_data
        3. 调用 _build_metadata_map 初始�?
        4. 使用不存在的 role_id 调用 get_rls_policies
        5. 验证抛出 SecurityPolicyNotFound

        【预期结果�?
        1. 抛出 SecurityPolicyNotFound 异常
        2. 异常�?role_id 字段�?"ROLE_NOT_EXIST"
        """
        registry = SemanticRegistry()
        
        yaml_data = {
            "security": {
                "role_policies": [
                    {
                        "policy_id": "POLICY_ROLE_SALES_STAFF",
                        "role_id": "ROLE_SALES_STAFF",
                        "scopes": {
                            "row_scope_code": "SELF"
                        }
                    }
                ]
            }
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 使用不存在的 role_id 调用 get_rls_policies
        with pytest.raises(SecurityPolicyNotFound) as exc_info:
            registry.get_rls_policies(
                role_id="ROLE_NOT_EXIST",
                entity_id="ENT_SALES_ORDER_ITEM",
                user_id="1001",
                tenant_id="tenant_001"
            )
        
        # 验证异常信息
        assert exc_info.value.role_id == "ROLE_NOT_EXIST"


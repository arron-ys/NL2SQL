import pytest
from nl2sql_service.core.semantic_registry import SemanticRegistry, SecurityConfigError

class TestRLSPoliciesFixed:
    """测试修复后的 RLS 策略生成（精确匹配 domain_id）"""
    
    @pytest.mark.unit
    def test_get_rls_policies_self_scope_with_entity_def(self):
        """测试 SELF scope 正确工作（包含 entity_def）"""
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
            ],
            "entities": [
                {
                    "id": "ENT_SALES_ORDER_ITEM",
                    "name": "销售订单明细",
                    "domain_id": "SALES"
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_SALES_STAFF",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        assert len(rls_sql_list) == 1
        assert "sales_rep_employee_number = 1001" in rls_sql_list[0]
        assert "{{" not in rls_sql_list[0]
        assert "}}" not in rls_sql_list[0]
    
    @pytest.mark.unit
    def test_get_rls_policies_dept_scope_with_entity_def(self):
        """测试 DEPT scope 正确工作（包含 entity_def 和 tenant_id）"""
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
            ],
            "entities": [
                {
                    "id": "ENT_SALES_ORDER_ITEM",
                    "name": "销售订单明细",
                    "domain_id": "SALES"
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_SALES_HEAD",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        assert len(rls_sql_list) == 1
        sql = rls_sql_list[0]
        assert "FROM dim_org_scope WHERE manager_id = 1001" in sql
        assert "AND tenant_id = 'tenant_001'" in sql
        assert "{{" not in sql
        assert "}}" not in sql
    
    @pytest.mark.unit
    def test_get_rls_policies_dept_scope_selects_correct_domain_binding(self):
        """验证精确匹配 domain_id 的选择逻辑"""
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
                    "fragment_id": "FRAG_HR_DEPT_ORDER_RLS",
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
                            "domain_id": "HR",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": "FRAG_HR_DEPT_ORDER_RLS"
                        }
                    ]
                }
            ],
            "entities": [
                {
                    "id": "ENT_SALES_ORDER_ITEM",
                    "name": "销售订单明细",
                    "domain_id": "SALES"
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_SALES_HEAD",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        assert len(rls_sql_list) == 1
        sql = rls_sql_list[0]
        assert "FROM dim_org_scope WHERE manager_id = 1001" in sql
        assert "AND tenant_id = 'tenant_001'" in sql
        assert "sales_rep_employee_number" in sql
        assert "hr_table" not in sql
    
    @pytest.mark.unit
    def test_get_rls_policies_non_company_scope_null_fragment_ref_fail_closed(self):
        """验证非 COMPANY scope 的 fragment_ref 为 null 时 fail-closed"""
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
            "policy_fragments": [],  # 故意不定义任何 fragment
            "row_scope_bindings": [
                {
                    "row_scope_code": "DEPT",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": None  # DEPT scope 的 fragment_ref 为 null（错误配置）
                        }
                    ]
                }
            ],
            "entities": [
                {
                    "id": "ENT_SALES_ORDER_ITEM",
                    "name": "销售订单明细",
                    "domain_id": "SALES"
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 调用 get_rls_policies 应该抛出 SecurityConfigError
        with pytest.raises(SecurityConfigError) as exc_info:
            registry.get_rls_policies(
                role_id="ROLE_SALES_HEAD",
                entity_id="ENT_SALES_ORDER_ITEM",
                user_id="1001",
                tenant_id="tenant_001"
            )
        
        # 验证错误信息包含关键信息
        error_msg = str(exc_info.value)
        assert "fragment_ref is null for non-COMPANY scope" in error_msg
        assert "role_id=ROLE_SALES_HEAD" in error_msg
        assert "entity_id=ENT_SALES_ORDER_ITEM" in error_msg
        assert "row_scope_code=DEPT" in error_msg
        assert "domain_id=SALES" in error_msg
        assert "binding_key=" in error_msg
    
    @pytest.mark.unit
    def test_get_rls_policies_company_scope_null_fragment_ref_allowed(self):
        """验证 COMPANY scope 的 fragment_ref 为 null 时允许返回空列表"""
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
            "policy_fragments": [],  # 不需要定义 fragment
            "row_scope_bindings": [
                {
                    "row_scope_code": "COMPANY",
                    "bindings": [
                        {
                            "domain_id": "SALES",
                            "entity_id": "ENT_SALES_ORDER_ITEM",
                            "fragment_ref": None  # COMPANY scope 的 fragment_ref 为 null（正确配置）
                        }
                    ]
                }
            ],
            "entities": [
                {
                    "id": "ENT_SALES_ORDER_ITEM",
                    "name": "销售订单明细",
                    "domain_id": "SALES"
                }
            ]
        }
        
        registry._build_metadata_map(yaml_data)
        
        # 调用 get_rls_policies 应该返回空列表
        rls_sql_list = registry.get_rls_policies(
            role_id="ROLE_CEO",
            entity_id="ENT_SALES_ORDER_ITEM",
            user_id="1001",
            tenant_id="tenant_001"
        )
        
        # 验证返回空列表
        assert len(rls_sql_list) == 0

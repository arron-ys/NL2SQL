"""
Unit tests for SemanticRegistry time_field_id reverse index (Step 2)

Tests:
1. test_time_field_id_unique_enforced - Duplicate time_field_id raises error
2. test_default_time_grain_must_be_allowed - default_time_grain validation
3. test_resolve_dimension_id_by_time_field_id - Reverse lookup works correctly
"""
import pytest
from unittest.mock import MagicMock

pytestmark = pytest.mark.unit

from core.semantic_registry import SemanticRegistry, SemanticConfigurationError


def test_time_field_id_unique_enforced():
    """Test that duplicate time_field_id across dimensions raises SemanticConfigurationError"""
    registry = SemanticRegistry()
    
    # YAML data with duplicate time_field_id
    yaml_data = {
        "global_config": {},
        "security": {},
        "metrics": [],
        "dimensions": [
            {
                "id": "DIM_ORDER_DATE",
                "name": "订单日期",
                "is_time_dimension": True,
                "time_field_id": "ORDER_DATE",
                "allowed_time_grains": ["DAY", "MONTH", "YEAR"]
            },
            {
                "id": "DIM_CREATED_DATE",
                "name": "创建日期",
                "is_time_dimension": True,
                "time_field_id": "ORDER_DATE",  # Duplicate!
                "allowed_time_grains": ["DAY", "MONTH"]
            }
        ],
        "entities": [],
        "enums": [],
        "logical_filters": []
    }
    
    # Should raise SemanticConfigurationError
    with pytest.raises(SemanticConfigurationError) as exc_info:
        registry._build_metadata_map(yaml_data)
    
    assert "Duplicate time_field_id" in str(exc_info.value)
    assert "ORDER_DATE" in str(exc_info.value)
    assert exc_info.value.details["time_field_id"] == "ORDER_DATE"
    assert exc_info.value.details["dimension_1"] == "DIM_ORDER_DATE"
    assert exc_info.value.details["dimension_2"] == "DIM_CREATED_DATE"


def test_default_time_grain_must_be_allowed():
    """Test that default_time_grain must be in allowed_time_grains (Step 3 validation)"""
    registry = SemanticRegistry()
    
    # Test 1: default_time_grain not in allowed_time_grains - should raise error
    yaml_data_invalid = {
        "global_config": {},
        "security": {},
        "metrics": [],
        "dimensions": [
            {
                "id": "DIM_ORDER_DATE",
                "name": "订单日期",
                "is_time_dimension": True,
                "time_field_id": "ORDER_DATE",
                "allowed_time_grains": ["DAY", "MONTH", "YEAR"],
                "default_time_grain": "WEEK"  # Not in allowed_time_grains!
            }
        ],
        "entities": [],
        "enums": [],
        "logical_filters": []
    }
    
    with pytest.raises(SemanticConfigurationError) as exc_info:
        registry._build_metadata_map(yaml_data_invalid)
    
    assert "invalid default_time_grain" in str(exc_info.value)
    assert "WEEK" in str(exc_info.value)
    assert exc_info.value.details["default_time_grain"] == "WEEK"
    assert exc_info.value.details["allowed_time_grains"] == ["DAY", "MONTH", "YEAR"]
    
    # Test 2: default_time_grain in allowed_time_grains - should succeed
    yaml_data_valid = {
        "global_config": {},
        "security": {},
        "metrics": [],
        "dimensions": [
            {
                "id": "DIM_ORDER_DATE",
                "name": "订单日期",
                "is_time_dimension": True,
                "time_field_id": "ORDER_DATE",
                "allowed_time_grains": ["DAY", "MONTH", "YEAR"],
                "default_time_grain": "DAY"  # Valid!
            }
        ],
        "entities": [],
        "enums": [],
        "logical_filters": []
    }
    
    # Should not raise
    registry._build_metadata_map(yaml_data_valid)
    
    # Test 3: default_time_grain without allowed_time_grains - should raise error
    yaml_data_no_allowed = {
        "global_config": {},
        "security": {},
        "metrics": [],
        "dimensions": [
            {
                "id": "DIM_ORDER_DATE",
                "name": "订单日期",
                "is_time_dimension": True,
                "time_field_id": "ORDER_DATE",
                "default_time_grain": "DAY"  # No allowed_time_grains!
            }
        ],
        "entities": [],
        "enums": [],
        "logical_filters": []
    }
    
    with pytest.raises(SemanticConfigurationError) as exc_info:
        registry._build_metadata_map(yaml_data_no_allowed)
    
    assert "has default_time_grain but no allowed_time_grains" in str(exc_info.value)


def test_resolve_dimension_id_by_time_field_id():
    """Test that resolve_dimension_id_by_time_field_id correctly returns dimension ID"""
    registry = SemanticRegistry()
    
    # YAML data with valid time dimensions
    yaml_data = {
        "global_config": {},
        "security": {},
        "metrics": [],
        "dimensions": [
            {
                "id": "DIM_ORDER_DATE",
                "name": "订单日期",
                "is_time_dimension": True,
                "time_field_id": "ORDER_DATE",
                "allowed_time_grains": ["DAY", "MONTH", "YEAR"]
            },
            {
                "id": "DIM_CREATED_DATE",
                "name": "创建日期",
                "is_time_dimension": True,
                "time_field_id": "CREATED_DATE",
                "allowed_time_grains": ["DAY", "MONTH"]
            },
            {
                "id": "DIM_REGION",
                "name": "地区",
                "is_time_dimension": False,  # Not a time dimension
                "column": "region"
            }
        ],
        "entities": [],
        "enums": [],
        "logical_filters": []
    }
    
    registry._build_metadata_map(yaml_data)
    
    # Test successful lookups
    assert registry.resolve_dimension_id_by_time_field_id("ORDER_DATE") == "DIM_ORDER_DATE"
    assert registry.resolve_dimension_id_by_time_field_id("CREATED_DATE") == "DIM_CREATED_DATE"
    
    # Test non-existent time_field_id
    assert registry.resolve_dimension_id_by_time_field_id("NON_EXISTENT") is None
    
    # Test that non-time dimensions are not indexed
    assert registry.resolve_dimension_id_by_time_field_id("region") is None

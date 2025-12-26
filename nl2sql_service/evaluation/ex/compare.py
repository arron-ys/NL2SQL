from typing import Dict, Any, Optional


NUMERIC_TOLERANCE = 1e-4


def cell_compare(pred_cell: str, gold_cell: str) -> bool:
    """
    Compare two canonical cells (both are strings).
    
    Applies numeric tolerance of 1e-4 for numeric values.
    
    Args:
        pred_cell: Predicted cell value (canonicalized string)
        gold_cell: Gold cell value (canonicalized string)
        
    Returns:
        True if match, False otherwise
    """
    # Step 1: Exact string match (fast path)
    if pred_cell == gold_cell:
        return True
    
    # Step 2: Try numeric comparison with tolerance
    # Skip special markers
    if pred_cell in ["<NULL>", "<INF>", "<-INF>"]:
        return False  # Already failed exact match
    if gold_cell in ["<NULL>", "<INF>", "<-INF>"]:
        return False
    
    try:
        pred_num = float(pred_cell)
        gold_num = float(gold_cell)
        
        # Apply tolerance: |pred - gold| <= 1e-4
        return abs(pred_num - gold_num) <= NUMERIC_TOLERANCE
    
    except ValueError:
        # Not numeric, already failed exact match
        return False


def compare_tables(
    pred_table: Dict[str, Any],
    gold_table: Dict[str, Any],
    order_sensitive: bool = False
) -> Dict[str, Any]:
    """
    Compare two canonicalized tables.
    
    Args:
        pred_table: Predicted table (canonicalized)
        gold_table: Gold table (canonicalized)
        order_sensitive: Whether row order matters
        
    Returns:
        Dict with 'match' (bool), 'reason' (str), and optional 'detail' (str)
    """
    pred_columns = pred_table["columns"]
    gold_columns = gold_table["columns"]
    pred_rows = pred_table["rows"]
    gold_rows = gold_table["rows"]
    
    # Step 1: Check truncation
    pred_truncated = pred_table.get("is_truncated", False)
    gold_truncated = gold_table.get("is_truncated", False)
    
    if pred_truncated or gold_truncated:
        return {
            "match": False,
            "reason": "TRUNCATED_UNSCORABLE",
            "detail": f"pred_truncated={pred_truncated}, gold_truncated={gold_truncated}"
        }
    
    # Step 2: Check column match
    if pred_columns != gold_columns:
        return {
            "match": False,
            "reason": "COLUMN_MISMATCH",
            "detail": f"pred_columns={pred_columns}, gold_columns={gold_columns}"
        }
    
    # Step 3: Check shape match
    if len(pred_rows) != len(gold_rows):
        return {
            "match": False,
            "reason": "SHAPE_MISMATCH",
            "detail": f"pred_shape=({len(pred_rows)}, {len(pred_columns)}), gold_shape=({len(gold_rows)}, {len(gold_columns)})"
        }
    
    # Step 4: Sort rows if order_sensitive=False
    # Note: pred_table and gold_table should already be sorted by caller if needed
    # This function works on already-sorted tables
    
    # Step 5: Compare rows cell by cell
    for i in range(len(pred_rows)):
        for j in range(len(pred_columns)):
            pred_cell = pred_rows[i][j]
            gold_cell = gold_rows[i][j]
            
            if not cell_compare(pred_cell, gold_cell):
                col_name = pred_columns[j] if j < len(pred_columns) else f"col_{j}"
                
                # Try to compute diff for numeric values
                diff_info = ""
                try:
                    pred_num = float(pred_cell)
                    gold_num = float(gold_cell)
                    diff = abs(pred_num - gold_num)
                    if gold_num != 0:
                        pct = (diff / abs(gold_num)) * 100
                        diff_info = f", diff={diff:.6f} ({pct:.2f}%)"
                    else:
                        diff_info = f", diff={diff:.6f}"
                except ValueError:
                    pass
                
                return {
                    "match": False,
                    "reason": "VALUE_MISMATCH",
                    "detail": f"row={i}, col='{col_name}', pred='{pred_cell}', gold='{gold_cell}'{diff_info}"
                }
    
    # Step 6: All match
    return {
        "match": True,
        "reason": "EXACT_MATCH"
    }

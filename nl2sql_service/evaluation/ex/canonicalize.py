import json
import math
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List
from dateutil import parser as dateutil_parser


def canonicalize_column(col: str) -> str:
    """
    Canonicalize column name: lower + strip
    
    Args:
        col: Column name string
        
    Returns:
        Canonicalized column name
    """
    return col.lower().strip()


def canonicalize_cell(cell: Any) -> str:
    """
    Canonicalize a single cell value according to EX spec.
    
    All values are converted to strings for comparison.
    Numeric values use absolute tolerance of 1e-4.
    
    Args:
        cell: Cell value of any type
        
    Returns:
        Canonicalized string representation
    """
    # Handle None
    if cell is None:
        return "<NULL>"
    
    # Handle NaN (check before numeric processing)
    if isinstance(cell, float) and math.isnan(cell):
        return "<NULL>"
    
    # Handle infinity
    if isinstance(cell, float):
        if math.isinf(cell):
            return "<INF>" if cell > 0 else "<-INF>"
    
    # Handle boolean (before numeric, as bool is subclass of int)
    if isinstance(cell, bool):
        return str(int(cell))
    
    # Handle numeric types (int, float, Decimal)
    if isinstance(cell, (int, float, Decimal)):
        return str(float(cell))
    
    # Handle datetime objects
    if isinstance(cell, datetime):
        return cell.isoformat()
    
    # Handle date objects
    if isinstance(cell, date):
        return cell.isoformat()
    
    # Handle bytes
    if isinstance(cell, bytes):
        try:
            text = cell.decode('utf-8')
            return text.lower().strip()
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode bytes: {e}")
    
    # Handle dict
    if isinstance(cell, dict):
        return json.dumps(cell, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    
    # Handle list
    if isinstance(cell, list):
        return json.dumps(cell, ensure_ascii=False, separators=(',', ':'))
    
    # Handle string
    if isinstance(cell, str):
        # Try to parse as numeric string
        try:
            num = float(cell)
            # Check for special values
            if math.isnan(num):
                return "<NULL>"
            if math.isinf(num):
                return "<INF>" if num > 0 else "<-INF>"
            return str(num)
        except ValueError:
            pass
        
        # Try to parse as datetime string
        try:
            # Strict parsing first (ISO 8601 and common SQL formats)
            dt = _parse_datetime_strict(cell)
            if dt:
                return dt.isoformat()
        except Exception:
            pass
        
        # Fallback: try flexible parsing
        try:
            dt = dateutil_parser.parse(cell)
            return dt.isoformat()
        except Exception:
            pass
        
        # Treat as regular text
        return cell.lower().strip()
    
    # Fallback: convert to string and apply text rules
    return str(cell).lower().strip()


def _parse_datetime_strict(s: str) -> Any:
    """
    Strict datetime parsing for common formats.
    
    Returns datetime object or None if parsing fails.
    """
    strict_formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
    ]
    
    for fmt in strict_formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    
    return None


def validate_table_structure(table: Dict[str, Any]) -> None:
    """
    Validate table structure.
    
    Args:
        table: Table dict with 'columns' and 'rows'
        
    Raises:
        ValueError: If structure is invalid
    """
    if not isinstance(table, dict):
        raise ValueError(f"Table must be dict, got {type(table)}")
    
    if "columns" not in table:
        raise ValueError("Table missing 'columns' field")
    
    if "rows" not in table:
        raise ValueError("Table missing 'rows' field")
    
    columns = table["columns"]
    rows = table["rows"]
    
    if not isinstance(columns, list):
        raise ValueError(f"columns must be list, got {type(columns)}")
    
    if not isinstance(rows, list):
        raise ValueError(f"rows must be list, got {type(rows)}")
    
    # Check all column names are strings
    for i, col in enumerate(columns):
        if not isinstance(col, str):
            raise ValueError(f"Column {i} must be string, got {type(col)}")
    
    # Check all rows are lists with correct length
    for i, row in enumerate(rows):
        if not isinstance(row, list):
            raise ValueError(f"Row {i} must be list, got {type(row)}")
        if len(row) != len(columns):
            raise ValueError(
                f"Row {i} has {len(row)} cells but expected {len(columns)} (columns count)"
            )


def canonicalize_table(table: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonicalize entire table (columns + rows).
    
    Args:
        table: Table dict with 'columns' and 'rows'
        
    Returns:
        Canonicalized table with all cells as strings
    """
    # Step 1: Validate structure
    validate_table_structure(table)
    
    # Step 2: Canonicalize columns
    canonical_columns = [canonicalize_column(col) for col in table["columns"]]
    
    # Step 3: Canonicalize cells
    canonical_rows = []
    for row in table["rows"]:
        canonical_row = [canonicalize_cell(cell) for cell in row]
        canonical_rows.append(canonical_row)
    
    # Step 4: Build canonical table
    return {
        "columns": canonical_columns,
        "rows": canonical_rows,
        "is_truncated": table.get("is_truncated", False)
    }


def stable_sort_rows(canonical_table: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply stable sort to table rows (for order_sensitive=False).
    
    Sorts by all columns left to right using lexicographic order.
    
    Args:
        canonical_table: Canonicalized table
        
    Returns:
        Table with sorted rows
    """
    columns = canonical_table["columns"]
    rows = canonical_table["rows"]
    
    if not rows:
        return canonical_table
    
    # Sort by all columns (lexicographic order)
    # Python's sort is stable by default
    sorted_rows = sorted(rows, key=lambda row: tuple(row))
    
    return {
        "columns": columns,
        "rows": sorted_rows,
        "is_truncated": canonical_table.get("is_truncated", False)
    }

import duckdb
from typing import List

def assert_min_rows(rel_or_path: str, min_rows: int):
    """Ensure minimum number of rows"""
    
    conn = duckdb.connect()
    
    if isinstance(rel_or_path, str):
        count_result = conn.execute(f"SELECT COUNT(*) FROM '{rel_or_path}'").fetchone()
    else:
        # Assume it's a DuckDB relation
        count_result = rel_or_path.execute("SELECT COUNT(*)").fetchone()
    
    actual_rows = count_result[0]
    
    if actual_rows < min_rows:
        raise ValueError(f"Row count check failed: {actual_rows} < {min_rows} (minimum)")
    
    print(f"✓ Row count check passed: {actual_rows} >= {min_rows}")

def assert_null_ratio(rel_or_path: str, columns: List[str], max_ratio: float):
    """Check null ratio for specified columns"""
    
    conn = duckdb.connect()
    
    for column in columns:
        if isinstance(rel_or_path, str):
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({column}) as non_null_rows,
                (COUNT(*) - COUNT({column})) * 1.0 / COUNT(*) as null_ratio
            FROM '{rel_or_path}'
            """
            result = conn.execute(query).fetchone()
        else:
            # Assume it's a DuckDB relation  
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({column}) as non_null_rows, 
                (COUNT(*) - COUNT({column})) * 1.0 / COUNT(*) as null_ratio
            """
            result = rel_or_path.execute(query).fetchone()
        
        total_rows, non_null_rows, null_ratio = result
        
        if null_ratio > max_ratio:
            raise ValueError(f"Null ratio check failed for column '{column}': {null_ratio:.3f} > {max_ratio} (maximum)")
        
        print(f"✓ Null ratio check passed for '{column}': {null_ratio:.3f} <= {max_ratio}")

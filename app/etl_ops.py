import os
import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
from .io_sources import test_file_exists, read_file_to_duckdb, rest_fetch

def evaluate_condition(condition: str, env: str) -> bool:
    """Evaluate enabled_if condition"""
    # Simple condition evaluator for MVP
    if condition == "true":
        return True
    elif condition == "false":
        return False
    elif "env ==" in condition:
        # Extract env value: "env == 'dev'" -> 'dev'
        target_env = condition.split("==")[1].strip().strip("'\"")
        return env == target_env
    else:
        # Default to false for unknown conditions
        print(f"Warning: Unknown condition '{condition}', defaulting to False")
        return False

def select_source(spec: Dict, env: str) -> str:
    """Select appropriate data source based on environment and availability"""
    
    # Get primary source list
    primary_sources = spec['source_routing']['primary']
    
    # Find enabled sources
    enabled_sources = []
    for source in spec['sources']:
        if evaluate_condition(source['enabled_if'], env):
            enabled_sources.append(source)
    
    # Select first available source from priority list
    for source_name in primary_sources:
        for source in enabled_sources:
            if source['name'] == source_name:
                # Test connectivity/availability
                if test_source_connectivity(source):
                    return source_name
                else:
                    print(f"Source {source_name} failed connectivity test, trying next")
    
    # If no source available, raise error
    available = [s['name'] for s in enabled_sources]
    raise ValueError(f"No available sources found. Enabled: {available}, Primary: {primary_sources}")

def test_source_connectivity(source: Dict) -> bool:
    """Test if source is accessible"""
    try:
        if source['type'] == 'file':
            return test_file_exists(source['path'])
        elif source['type'] == 'rest':
            import requests
            url = source['base_url'].rstrip('/') + '/' + source['path'].lstrip('/')
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        elif source['type'] == 'postgres':
            # For MVP, assume postgres is available if specified
            return True
        else:
            return False
    except Exception as e:
        print(f"Connectivity test failed for {source['name']}: {e}")
        return False

def extract(source: Dict) -> str:
    """Extract data from source, return path to temporary file"""
    
    if source['type'] == 'file':
        return read_file_to_duckdb(
            source['path'], 
            source['format'], 
            source.get('options', {})
        )
    elif source['type'] == 'rest':
        return rest_fetch(
            source['base_url'],
            source['path'],
            source.get('pagination'),
            source.get('since')
        )
    elif source['type'] == 'postgres':
        from .io_sources import pg_fetch
        return pg_fetch(
            source['conn_id'],
            source['table'],
            source.get('mode', 'incremental'),
            source.get('updated_at')
        )
    else:
        raise ValueError(f"Unknown source type: {source['type']}")

def run_sql(data_path: str, sql_path: str, params: Dict[str, str]) -> str:
    """Apply SQL transformation to data"""
    
    # Read SQL file
    with open(sql_path, 'r') as f:
        sql_template = f.read()
    
    # Replace parameters in SQL
    sql = sql_template
    for key, value in params.items():
        sql = sql.replace(f"${{{key}}}", str(value))
    
    # Create output path
    temp_dir = Path("/tmp/etl_pipeline")
    temp_dir.mkdir(exist_ok=True)
    output_path = temp_dir / f"transformed_{hash(sql_path)}.parquet"
    
    # Execute SQL with DuckDB
    conn = duckdb.connect()
    
    # Create source_data view from input file
    conn.execute(f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM '{data_path}'")
    
    # Execute transform SQL and save result
    conn.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT PARQUET)")
    
    conn.close()
    
    print(f"Transform completed: {output_path}")
    return str(output_path)

def write_sqlite(rel_or_path: str, db_path: str, table: str, mode: str = "merge") -> str:
    """Write data to SQLite database"""
    
    # Ensure output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to SQLite via DuckDB
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{db_path}' AS sqlite_db (TYPE sqlite)")
    
    if mode == "merge":
        # For merge mode, drop and recreate table (simplified for MVP)
        conn.execute(f"DROP TABLE IF EXISTS sqlite_db.{table}")
        conn.execute(f"CREATE TABLE sqlite_db.{table} AS SELECT * FROM '{rel_or_path}'")
    elif mode == "append":
        # Try to insert, create table if doesn't exist
        try:
            conn.execute(f"INSERT INTO sqlite_db.{table} SELECT * FROM '{rel_or_path}'")
        except:
            conn.execute(f"CREATE TABLE sqlite_db.{table} AS SELECT * FROM '{rel_or_path}'")
    else:
        raise ValueError(f"Unknown SQLite mode: {mode}")
    
    conn.close()
    
    print(f"SQLite write completed: {db_path}#{table}")
    return f"{db_path}#{table}"

def write_parquet(rel_or_path: str, output_dir: str, partition_by: list = None) -> str:
    """Write data to Parquet files with optional partitioning"""
    
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect()
    
    if partition_by and len(partition_by) > 0:
        # Write with partitioning
        partition_cols = ", ".join(partition_by)
        query = f"COPY (SELECT * FROM '{rel_or_path}') TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY ({partition_cols}))"
    else:
        # Write single file
        output_file = Path(output_dir) / "data.parquet"
        query = f"COPY (SELECT * FROM '{rel_or_path}') TO '{output_file}' (FORMAT PARQUET)"
    
    conn.execute(query)
    conn.close()
    
    print(f"Parquet write completed: {output_dir}")
    return str(output_dir)
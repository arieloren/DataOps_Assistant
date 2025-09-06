import os
import glob
import requests
import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import urljoin

def test_file_exists(pattern: str) -> bool:
    """Check if at least one file matches the pattern"""
    matches = glob.glob(pattern)
    return len(matches) > 0

def read_file_to_duckdb(pattern: str, fmt: str, options: Dict[str, Any]) -> str:
    """Read file(s) matching pattern into temporary table, return path"""
    
    if not test_file_exists(pattern):
        raise FileNotFoundError(f"No files found matching pattern: {pattern}")
    
    # Create temp file path
    temp_dir = Path("/tmp/etl_pipeline")
    temp_dir.mkdir(exist_ok=True)
    temp_file = temp_dir / f"extracted_{hash(pattern)}.parquet"
    
    # Use DuckDB to read and consolidate files
    conn = duckdb.connect()
    
    if fmt.lower() == 'csv':
        # Build CSV read options
        read_options = []
        if options.get('header', True):
            read_options.append("header=true")
        
        options_str = ", ".join(read_options) if read_options else ""
        
        if options_str:
            query = f"COPY (SELECT * FROM read_csv_auto('{pattern}', {options_str})) TO '{temp_file}' (FORMAT PARQUET)"
        else:
            query = f"COPY (SELECT * FROM read_csv_auto('{pattern}')) TO '{temp_file}' (FORMAT PARQUET)"
    else:
        raise ValueError(f"Unsupported file format: {fmt}")
    
    conn.execute(query)
    conn.close()
    
    print(f"File data consolidated to: {temp_file}")
    return str(temp_file)

def rest_fetch(base_url: str, path: str, pagination: Optional[Dict] = None, since: Optional[Dict] = None) -> str:
    """Fetch data from REST API, return temporary parquet file path"""
    
    url = urljoin(base_url, path)
    all_data = []
    page = 1
    
    # Setup pagination parameters
    page_param = pagination.get('param', 'page') if pagination else 'page'
    page_size = pagination.get('size', 100) if pagination else 100
    
    print(f"Fetching from REST API: {url}")
    
    while True:
        # Build request parameters
        params = {page_param: page, 'limit': page_size}
        
        # Add since parameter if configured
        if since:
            # For MVP, use a default since value - in production this would come from state
            params[since['field']] = '2024-01-01'
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Try common pagination response patterns
                records = data.get('data', data.get('results', data.get('items', [])))
                if not isinstance(records, list):
                    records = [data]  # Single record response
            else:
                raise ValueError(f"Unexpected response format: {type(data)}")
            
            if not records:
                break  # No more data
                
            all_data.extend(records)
            
            # Check if we should continue paginating
            if not pagination or len(records) < page_size:
                break
                
            page += 1
            
            # Safety limit for MVP
            if page > 100:
                print("Warning: Hit pagination safety limit of 100 pages")
                break
                
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to fetch from {url}: {e}")
    
    if not all_data:
        raise ValueError(f"No data received from {url}")
    
    # Convert to DataFrame and save as Parquet
    df = pd.DataFrame(all_data)
    
    temp_dir = Path("/tmp/etl_pipeline")  
    temp_dir.mkdir(exist_ok=True)
    temp_file = temp_dir / f"rest_{hash(url)}_{page}.parquet"
    
    df.to_parquet(temp_file, index=False)
    
    print(f"REST data saved to: {temp_file} ({len(all_data)} records)")
    return str(temp_file)

def pg_fetch(conn_id: str, table: str, mode: str = "incremental", updated_at: Optional[str] = None) -> str:
    """Fetch data from PostgreSQL - placeholder for Phase A"""
    # For Phase A, this is optional - just return error
    raise NotImplementedError("PostgreSQL connector not implemented in Phase A MVP")

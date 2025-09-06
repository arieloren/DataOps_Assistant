import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

def init_catalog_db(catalog_db_path: str):
    """Initialize catalog database with runs table"""
    
    # Ensure directory exists
    Path(catalog_db_path).parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(catalog_db_path)
    
    # Create runs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            rows_out INTEGER,
            output_paths TEXT,  -- JSON array
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            duration_seconds REAL,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

def record_run(catalog_db: str, pipeline: str, status: str, 
               rows_out: Optional[int] = None, 
               output_paths: Optional[List[str]] = None,
               started_at: Optional[datetime] = None,
               ended_at: Optional[datetime] = None,
               error_message: Optional[str] = None):
    """Record pipeline run in catalog"""
    
    # Initialize catalog if needed
    init_catalog_db(catalog_db)
    
    # Calculate duration
    duration_seconds = None
    if started_at and ended_at:
        duration_seconds = (ended_at - started_at).total_seconds()
    
    # Serialize output paths
    output_paths_json = json.dumps(output_paths or [])
    
    conn = sqlite3.connect(catalog_db)
    
    conn.execute("""
        INSERT INTO runs (
            pipeline, status, rows_out, output_paths, 
            started_at, ended_at, duration_seconds, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline, status, rows_out, output_paths_json,
        started_at, ended_at, duration_seconds, error_message
    ))
    
    conn.commit()
    run_id = conn.lastrowid
    conn.close()
    
    print(f"✓ Catalog updated - run_id: {run_id}, status: {status}")
    return run_id

def get_pipeline_runs(catalog_db: str, pipeline: str, limit: int = 10) -> List[dict]:
    """Get recent runs for a pipeline"""
    
    if not Path(catalog_db).exists():
        return []
    
    conn = sqlite3.connect(catalog_db)
    conn.row_factory = sqlite3.Row  # Enable dict-like access
    
    cursor = conn.execute("""
        SELECT * FROM runs 
        WHERE pipeline = ? 
        ORDER BY started_at DESC 
        LIMIT ?
    """, (pipeline, limit))
    
    runs = [dict(row) for row in cursor.fetchall()]
    
    # Parse output_paths back to list
    for run in runs:
        run['output_paths'] = json.loads(run['output_paths'] or '[]')
    
    conn.close()
    return runs

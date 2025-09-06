import pytest
import tempfile
import json
import os
import pandas as pd
from pathlib import Path
import shutil
from unittest.mock import Mock

@pytest.fixture
def temp_workspace():
    """Create temporary workspace with full directory structure"""
    temp_dir = tempfile.mkdtemp(prefix="etl_test_")
    
    # Create directory structure
    directories = [
        'specs', 'dags', 'sql', 'tests', 'configs', 'datasets',
        'data/sqlite', 'data/parquet', 'data/catalog', 'app'
    ]
    
    for directory in directories:
        Path(temp_dir, directory).mkdir(parents=True, exist_ok=True)
    
    # Create sample CSV data
    sample_orders = pd.DataFrame({
        'order_id': [1001, 1002, 1003, 1002, 1004],  # Duplicate 1002
        'customer_id': [501, 502, 503, 502, 504],
        'item_id': [201, 202, 201, 202, 203],
        'quantity': [2, 1, 3, 1, 1],
        'order_date': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-15', '2024-01-17'],
        'updated_at': ['2024-01-15 10:30:00', '2024-01-15 11:45:00', '2024-01-16 09:15:00', 
                      '2024-01-15 12:00:00', '2024-01-17 16:45:00']  # Later time for duplicate
    })
    
    csv_path = Path(temp_dir, 'datasets', 'orders_sample.csv')
    sample_orders.to_csv(csv_path, index=False)
    
    # Create items data
    sample_items = pd.DataFrame({
        'item_id': [201, 202, 203, 204],
        'name': ['Widget A', 'Gadget B', 'Tool C', 'Device D'],
        'category': ['Electronics', 'Electronics', 'Hardware', 'Electronics'],
        'price': [29.99, 49.99, 19.99, 99.99]
    })
    
    items_path = Path(temp_dir, 'datasets', 'items_sample.csv')
    sample_items.to_csv(items_path, index=False)
    
    # Create DAG template
    dag_template = '''# Generated DAG for {{ spec.pipeline_name }}
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'etl-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '{{ spec.pipeline_name }}',
    default_args=default_args,
    description='Generated ETL pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'generated'],
)

def dummy_task():
    print("Pipeline: {{ spec.pipeline_name }}")
    return "success"

task = PythonOperator(
    task_id='dummy_task',
    python_callable=dummy_task,
    dag=dag,
)
'''
    
    template_path = Path(temp_dir, 'app', 'dag_template.py.j2')
    with open(template_path, 'w') as f:
        f.write(dag_template)
    
    yield temp_dir
    
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def sample_pipeline_spec():
    """Sample valid pipeline specification"""
    return {
        "pipeline_name": "test_orders_pipeline",
        "schedule": {
            "on_demand": True,
            "daily_02": False,
            "weekly_mon_06": False
        },
        "profile": "dev",
        "sources": [
            {
                "type": "file",
                "name": "orders_csv",
                "path": "./datasets/orders_*.csv",
                "format": "csv",
                "options": {"header": True},
                "enabled_if": "env == 'dev'"
            }
        ],
        "source_routing": {
            "primary": ["orders_csv"]
        },
        "transform": {
            "sql_file": "test_orders.sql",
            "params": {
                "dedupe_key": "order_id",
                "latest_col": "updated_at"
            }
        },
        "outputs": {
            "sqlite": {
                "path": "data/sqlite/test.db",
                "mode": "merge",
                "table": "orders"
            },
            "parquet": {
                "dir": "data/parquet/orders/",
                "partition_by": ["ingest_date"]
            }
        },
        "checks": {
            "min_rows": 1,
            "null_ratio": {
                "columns": ["order_id"],
                "max": 0.0
            }
        }
    }

@pytest.fixture
def mock_openai_response():
    """Mock OpenAI API response for testing"""
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = json.dumps({
        "pipeline_name": "mock_pipeline",
        "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
        "profile": "dev",
        "sources": [{
            "type": "file",
            "name": "mock_source",
            "path": "./test.csv",
            "format": "csv",
            "options": {"header": True},
            "enabled_if": "true"
        }],
        "source_routing": {"primary": ["mock_source"]},
        "transform": {"sql_file": "mock.sql", "params": {}},
        "outputs": {"sqlite": {"path": "test.db", "mode": "merge", "table": "test"}},
        "checks": {"min_rows": 1}
    })
    return mock_response

@pytest.fixture
def sample_rest_api_data():
    """Sample REST API response data"""
    return [
        {"id": 1, "name": "Item 1", "price": 10.99, "category": "A"},
        {"id": 2, "name": "Item 2", "price": 20.99, "category": "B"},
        {"id": 3, "name": "Item 3", "price": 30.99, "category": "A"},
    ]

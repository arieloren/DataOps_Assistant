"""
Test ETL operations (extract, transform, load)
"""
import pytest
import sys
import os
import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import patch, Mock
import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.etl_ops import select_source, evaluate_condition, extract, run_sql, write_sqlite, write_parquet, test_source_connectivity

class TestETLOperations:
    """Test core ETL operations"""
    
    def test_evaluate_condition_logic(self):
        """Test condition evaluation for source selection"""
        
        # Test simple conditions
        assert evaluate_condition("true", "dev") is True
        assert evaluate_condition("false", "dev") is False
        
        # Test environment conditions
        assert evaluate_condition("env == 'dev'", "dev") is True
        assert evaluate_condition("env == 'dev'", "prod") is False
        assert evaluate_condition("env == 'prod'", "prod") is True
        assert evaluate_condition("env == 'prod'", "dev") is False
    
    @patch('app.etl_ops.test_source_connectivity')
    def test_select_source_dev_environment(self, mock_connectivity):
        """Test source selection in dev environment"""
        
        mock_connectivity.return_value = True
        
        spec = {
            'sources': [
                {
                    'type': 'file',
                    'name': 'orders_csv',
                    'path': './datasets/orders_*.csv',
                    'enabled_if': "env == 'dev'"
                },
                {
                    'type': 'postgres',
                    'name': 'orders_pg', 
                    'enabled_if': "env == 'prod'"
                },
                {
                    'type': 'rest',
                    'name': 'orders_api',
                    'enabled_if': "true"
                }
            ],
            'source_routing': {
                'primary': ['orders_csv', 'orders_pg', 'orders_api']
            }
        }
        
        # In dev environment, should select orders_csv first
        selected = select_source(spec, 'dev')
        assert selected == 'orders_csv'
    
    @patch('app.etl_ops.test_source_connectivity')
    def test_select_source_fallback(self, mock_connectivity):
        """Test source selection fallback when first choice fails"""
        
        # Mock first source fails, second succeeds
        mock_connectivity.side_effect = [False, True]
        
        spec = {
            'sources': [
                {'type': 'file', 'name': 'source1', 'enabled_if': 'true'},
                {'type': 'file', 'name': 'source2', 'enabled_if': 'true'}
            ],
            'source_routing': {'primary': ['source1', 'source2']}
        }
        
        selected = select_source(spec, 'dev')
        assert selected == 'source2'
    
    def test_source_connectivity_file(self, temp_workspace):
        """Test file source connectivity checking"""
        
        # Test existing file
        csv_path = Path(temp_workspace) / 'datasets' / 'orders_sample.csv'
        assert csv_path.exists()
        
        file_source = {
            'type': 'file',
            'name': 'test_csv',
            'path': str(csv_path.parent / 'orders_*.csv')
        }
        
        assert test_source_connectivity(file_source) is True
        
        # Test non-existent file
        missing_source = {
            'type': 'file', 
            'name': 'missing',
            'path': '/nonexistent/path/*.csv'
        }
        
        assert test_source_connectivity(missing_source) is False
    
    def test_extract_from_csv_file(self, temp_workspace):
        """Test data extraction from CSV file"""
        
        csv_source = {
            'type': 'file',
            'name': 'orders_csv',
            'path': str(Path(temp_workspace) / 'datasets' / 'orders_*.csv'),
            'format': 'csv',
            'options': {'header': True}
        }
        
        result_path = extract(csv_source)
        
        assert Path(result_path).exists()
        
        # Verify data was extracted correctly
        conn = duckdb.connect()
        result = conn.execute(f"SELECT COUNT(*) FROM '{result_path}'").fetchone()
        assert result[0] == 5  # Sample data has 5 rows
        
        # Check column names
        columns = conn.execute(f"DESCRIBE SELECT * FROM '{result_path}'").fetchall()
        column_names = [col[0] for col in columns]
        expected_columns = ['order_id', 'customer_id', 'item_id', 'quantity', 'order_date', 'updated_at']
        
        for col in expected_columns:
            assert col in column_names
    
    @patch('requests.get')
    def test_extract_from_rest_api(self, mock_get, sample_rest_api_data):
        """Test data extraction from REST API"""
        
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_rest_api_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        rest_source = {
            'type': 'rest',
            'name': 'orders_api',
            'url': 'https://api.example.com/orders',
            'options': {'headers': {'Authorization': 'Bearer token'}}
        }

        # Call the extract function
        result_path = extract(rest_source)

        # Verify the extracted data
        assert Path(result_path).exists()

        # Check the data content
        conn = duckdb.connect()
        result = conn.execute(f"SELECT COUNT(*) FROM '{result_path}'").fetchone()
        assert result[0] == len(sample_rest_api_data)  # Ensure row count matches mock data

        # Check column names
        columns = conn.execute(f"DESCRIBE SELECT * FROM '{result_path}'").fetchall()
        column_names = [col[0] for col in columns]
        expected_columns = sample_rest_api_data[0].keys()

        for col in expected_columns:
            assert col in column_names

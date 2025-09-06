"""
Test A1 Parser (Natural Language → Pipeline Spec)
"""
import pytest
import json
import sys
from pathlib import Path
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.a1_parser import RequestParser

class TestA1Parser:
    """Test natural language parsing to pipeline specs"""
    
    @patch('app.router.LLMRouter.generate')
    def test_parse_simple_csv_request(self, mock_generate, temp_workspace):
        """Test parsing simple CSV load request"""
        
        # Mock LLM response
        mock_spec = {
            "pipeline_name": "simple_csv_load",
            "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [{
                "type": "file",
                "name": "data_csv",
                "path": "./datasets/data_*.csv",
                "format": "csv",
                "options": {"header": True},
                "enabled_if": "env == 'dev'"
            }],
            "source_routing": {"primary": ["data_csv"]},
            "transform": {"sql_file": "simple_csv_load.sql", "params": {}},
            "outputs": {
                "sqlite": {"path": "data/sqlite/output.db", "mode": "merge", "table": "data"}
            },
            "checks": {"min_rows": 1}
        }
        
        mock_generate.return_value = json.dumps(mock_spec)
        
        # Test parser
        os.environ['OPENAI_API_KEY'] = 'test-key'  # Mock API key
        parser = RequestParser()
        
        result = parser.parse_request("Load CSV files from datasets folder into SQLite")
        
        assert result['pipeline_name'] == 'simple_csv_load'
        assert len(result['sources']) == 1
        assert result['sources'][0]['type'] == 'file'
        assert result['outputs']['sqlite']['table'] == 'data'
    
    @patch('app.router.LLMRouter.generate')  
    def test_parse_complex_etl_request(self, mock_generate, temp_workspace):
        """Test parsing complex ETL request with schedule and multiple outputs"""
        
        mock_spec = {
            "pipeline_name": "daily_orders_etl",
            "schedule": {"on_demand": True, "daily_02": True, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [
                {
                    "type": "file",
                    "name": "orders_csv",
                    "path": "./datasets/orders_*.csv", 
                    "format": "csv",
                    "options": {"header": True},
                    "enabled_if": "env == 'dev'"
                },
                {
                    "type": "rest",
                    "name": "items_api",
                    "base_url": "http://localhost:3000",
                    "path": "/v1/items",
                    "pagination": {"param": "page", "size": 100},
                    "enabled_if": "true"
                }
            ],
            "source_routing": {"primary": ["orders_csv", "items_api"]},
            "transform": {
                "sql_file": "daily_orders_etl.sql",
                "params": {"dedupe_key": "order_id", "latest_col": "updated_at"}
            },
            "outputs": {
                "sqlite": {"path": "data/sqlite/warehouse.db", "mode": "merge", "table": "orders_enriched"},
                "parquet": {"dir": "data/parquet/orders/", "partition_by": ["ingest_date"]}
            },
            "checks": {
                "min_rows": 1,
                "null_ratio": {"columns": ["order_id"], "max": 0.0}
            }
        }
        
        mock_generate.return_value = json.dumps(mock_spec)
        
        os.environ['OPENAI_API_KEY'] = 'test-key'
        parser = RequestParser()
        
        prompt = "Load orders CSV daily at 2am, enrich with items API, dedupe by order_id, output SQLite and Parquet"
        result = parser.parse_request(prompt)
        
        assert result['pipeline_name'] == 'daily_orders_etl'
        assert result['schedule']['daily_02'] is True
        assert len(result['sources']) == 2
        assert 'sqlite' in result['outputs']
        assert 'parquet' in result['outputs']
        assert result['transform']['params']['dedupe_key'] == 'order_id'
    
    @patch('app.router.LLMRouter.generate')
    def test_validate_and_save_spec(self, mock_generate, temp_workspace, sample_pipeline_spec):
        """Test spec validation and file saving"""
        
        os.environ['OPENAI_API_KEY'] = 'test-key'
        parser = RequestParser()
        
        # Test save to file
        output_dir = str(Path(temp_workspace) / 'specs')
        spec_path = parser.validate_and_save_spec(sample_pipeline_spec, output_dir)
        
        assert Path(spec_path).exists()
        
        # Verify file content
        with open(spec_path, 'r') as f:
            saved_spec = json.load(f)
        
        assert saved_spec['pipeline_name'] == sample_pipeline_spec['pipeline_name']
        assert saved_spec == sample_pipeline_spec
    
    @patch('app.router.LLMRouter.generate')
    def test_invalid_llm_response_handling(self, mock_generate, temp_workspace):
        """Test handling of invalid LLM responses"""
        
        # Mock invalid JSON response
        mock_generate.return_value = "This is not valid JSON"
        
        os.environ['OPENAI_API_KEY'] = 'test-key'
        parser = RequestParser()
        
        with pytest.raises(ValueError, match="Failed to parse LLM response as JSON"):
            parser.parse_request("Some request")

"""
Test pipeline specification validation
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.spec_schema import PipelineSpec
from pydantic import ValidationError

class TestSpecValidation:
    """Test Pydantic schema validation"""
    
    def test_valid_spec_loads_successfully(self, sample_pipeline_spec):
        """Test that a valid spec loads without errors"""
        spec = PipelineSpec(**sample_pipeline_spec)
        
        assert spec.pipeline_name == "test_orders_pipeline"
        assert spec.profile == "dev"
        assert len(spec.sources) == 1
        assert spec.sources[0].type == "file"
        assert spec.schedule.on_demand is True
    
    def test_missing_required_fields_fails(self):
        """Test that missing required fields cause validation errors"""
        incomplete_spec = {
            "pipeline_name": "test",
            # Missing required fields: schedule, sources, etc.
        }
        
        with pytest.raises(ValidationError):
            PipelineSpec(**incomplete_spec)
    
    def test_invalid_source_type_fails(self):
        """Test that invalid source types are rejected"""
        invalid_spec = {
            "pipeline_name": "test",
            "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [{
                "type": "invalid_source_type",  # Invalid
                "name": "test",
                "path": "./test.csv"
            }],
            "source_routing": {"primary": ["test"]},
            "transform": {"sql_file": "test.sql", "params": {}},
            "outputs": {"sqlite": {"path": "test.db", "mode": "merge", "table": "test"}},
            "checks": {"min_rows": 1}
        }
        
        with pytest.raises(ValidationError, match="type"):
            PipelineSpec(**invalid_spec)
    
    def test_empty_sources_fails(self):
        """Test that empty sources list is rejected"""
        no_sources_spec = {
            "pipeline_name": "test",
            "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [],  # Empty sources
            "source_routing": {"primary": []},
            "transform": {"sql_file": "test.sql", "params": {}},
            "outputs": {"sqlite": {"path": "test.db", "mode": "merge", "table": "test"}},
            "checks": {"min_rows": 1}
        }
        
        with pytest.raises(ValidationError, match="At least one source is required"):
            PipelineSpec(**no_sources_spec)
    
    def test_file_source_validation(self):
        """Test file source specific validation"""
        file_spec = {
            "pipeline_name": "test",
            "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [{
                "type": "file",
                "name": "test_file",
                "path": "./data/*.csv",
                "format": "csv",
                "options": {"header": True, "delimiter": ","},
                "enabled_if": "env == 'dev'"
            }],
            "source_routing": {"primary": ["test_file"]},
            "transform": {"sql_file": "test.sql", "params": {}},
            "outputs": {"sqlite": {"path": "test.db", "mode": "merge", "table": "test"}},
            "checks": {"min_rows": 1}
        }
        
        spec = PipelineSpec(**file_spec)
        assert spec.sources[0].format == "csv"
        assert spec.sources[0].options["header"] is True
    
    def test_rest_source_validation(self):
        """Test REST source specific validation"""
        rest_spec = {
            "pipeline_name": "test",
            "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [{
                "type": "rest",
                "name": "test_api",
                "base_url": "https://api.example.com",
                "path": "/v1/data",
                "pagination": {"param": "page", "size": 50},
                "since": {"field": "updated_at", "mode": "incremental"},
                "enabled_if": "true"
            }],
            "source_routing": {"primary": ["test_api"]},
            "transform": {"sql_file": "test.sql", "params": {}},
            "outputs": {"sqlite": {"path": "test.db", "mode": "merge", "table": "test"}},
            "checks": {"min_rows": 1}
        }
        
        spec = PipelineSpec(**rest_spec)
        assert spec.sources[0].pagination.size == 50
        assert spec.sources[0].since.mode == "incremental"

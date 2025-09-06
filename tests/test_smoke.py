"""
Smoke tests - verify basic imports and setup work
"""
import pytest
import sys
import os
from pathlib import Path

# Add app to path for testing
sys.path.insert(0, str(Path(__file__).parent.parent))

class TestSmokeTests:
    """Basic smoke tests to verify setup"""
    
    def test_core_imports(self):
        """Test that core modules can be imported"""
        try:
            from app.spec_schema import PipelineSpec
            from app.router import LLMRouter
            from app.etl_ops import select_source, extract
            from app.checks import assert_min_rows
            from app.catalog import record_run
            assert True, "All core imports successful"
        except ImportError as e:
            pytest.fail(f"Import failed: {e}")
    
    def test_dependencies_available(self):
        """Test that required dependencies are available"""
        required_packages = ['duckdb', 'pandas', 'requests', 'pydantic', 'jinja2']
        
        missing = []
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing.append(package)
        
        if missing:
            pytest.fail(f"Missing required packages: {missing}")
    
    def test_environment_setup(self):
        """Test basic environment configuration"""
        # Should not crash even without OPENAI_API_KEY for smoke test
        from app.spec_schema import PipelineSpec
        
        # Test basic spec validation
        minimal_spec = {
            "pipeline_name": "smoke_test",
            "schedule": {"on_demand": True, "daily_02": False, "weekly_mon_06": False},
            "profile": "dev",
            "sources": [{
                "type": "file",
                "name": "test",
                "path": "./test.csv",
                "format": "csv",
                "options": {"header": True},
                "enabled_if": "true"
            }],
            "source_routing": {"primary": ["test"]},
            "transform": {"sql_file": "test.sql", "params": {}},
            "outputs": {"sqlite": {"path": "test.db", "mode": "merge", "table": "test"}},
            "checks": {"min_rows": 1}
        }
        
        # Should not raise validation error
        spec = PipelineSpec(**minimal_spec)
        assert spec.pipeline_name == "smoke_test"

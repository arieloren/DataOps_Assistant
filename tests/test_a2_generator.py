"""
Test A2 Generator (Pipeline Spec → Airflow DAG)
"""
import pytest
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.a2_generator import PipelineGenerator

class TestA2Generator:
    """Test pipeline code generation"""
    
    def test_load_valid_spec(self, temp_workspace, sample_pipeline_spec):
        """Test loading and validating pipeline specification"""
        
        # Save spec to file
        spec_path = Path(temp_workspace) / 'specs' / 'test_pipeline.json'
        with open(spec_path, 'w') as f:
            json.dump(sample_pipeline_spec, f)
        
        generator = PipelineGenerator()
        generator.base_dir = Path(temp_workspace)
        
        spec = generator.load_spec(str(spec_path))
        
        assert spec.pipeline_name == "test_orders_pipeline"
        assert len(spec.sources) == 1
    
    def test_generate_dag_from_spec(self, temp_workspace, sample_pipeline_spec):
        """Test DAG generation from specification"""
        
        generator = PipelineGenerator()
        generator.base_dir = Path(temp_workspace)
        
        # Load spec
        spec_path = Path(temp_workspace) / 'specs' / 'test_pipeline.json'
        with open(spec_path, 'w') as f:
            json.dump(sample_pipeline_spec, f)
        
        spec = generator.load_spec(str(spec_path))
        dag_code = generator.generate_dag(spec)
        
        # Verify DAG content
        assert 'test_orders_pipeline' in dag_code
        assert 'etl-pipeline' in dag_code
        assert 'generated' in dag_code.lower()
        assert 'PythonOperator' in dag_code or 'dummy_task' in dag_code
    
    def test_generate_sql_transformation(self, temp_workspace, sample_pipeline_spec):
        """Test SQL generation from specification"""
        
        generator = PipelineGenerator()
        generator.base_dir = Path(temp_workspace)
        
        spec_path = Path(temp_workspace) / 'specs' / 'test_pipeline.json'
        with open(spec_path, 'w') as f:
            json.dump(sample_pipeline_spec, f)
        
        spec = generator.load_spec(str(spec_path))
        sql_code = generator.generate_sql(spec)
        
        # Verify SQL content
        assert 'SELECT' in sql_code.upper()
        assert 'order_id' in sql_code  # From dedupe_key param
        assert 'updated_at' in sql_code  # From latest_col param
        assert 'ingest_date' in sql_code  # Standard field
    
    def test_generate_test_file(self, temp_workspace, sample_pipeline_spec):
        """Test test file generation"""
        
        generator = PipelineGenerator()
        generator.base_dir = Path(temp_workspace)
        
        spec_path = Path(temp_workspace) / 'specs' / 'test_pipeline.json'
        with open(spec_path, 'w') as f:
            json.dump(sample_pipeline_spec, f)
        
        spec = generator.load_spec(str(spec_path))
        test_code = generator.generate_test(spec)
        
        # Verify test content
        assert 'test_test_orders_pipeline' in test_code
        assert 'def test_' in test_code
        assert 'import pytest' in test_code
        assert 'PipelineSpec' in test_code
    
    def test_complete_pipeline_generation(self, temp_workspace, sample_pipeline_spec):
        """Test complete pipeline generation workflow"""
        
        # Save spec to file
        spec_path = Path(temp_workspace) / 'specs' / 'test_pipeline.json'
        with open(spec_path, 'w') as f:
            json.dump(sample_pipeline_spec, f)
        
        generator = PipelineGenerator()  
        generator.base_dir = Path(temp_workspace)
        
        # Generate complete pipeline
        generator.generate_pipeline(str(spec_path))
        
        # Verify all files were created
        expected_files = [
            Path(temp_workspace) / 'dags' / 'test_orders_pipeline.py',
            Path(temp_workspace) / 'sql' / 'test_orders.sql', 
            Path(temp_workspace) / 'tests' / 'test_test_orders_pipeline.py'
        ]
        
        for file_path in expected_files:
            assert file_path.exists(), f"Expected file not created: {file_path}"
            
            # Verify files have content
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

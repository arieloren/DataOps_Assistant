import argparse
import json
import os
from pathlib import Path
from jinja2 import Template

from .spec_schema import PipelineSpec

class PipelineGenerator:
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        
    def load_spec(self, spec_path: str) -> PipelineSpec:
        """Load and validate pipeline specification"""
        with open(spec_path, 'r') as f:
            spec_dict = json.load(f)
        return PipelineSpec(**spec_dict)
    
    def generate_dag(self, spec: PipelineSpec) -> str:
        """Generate Airflow DAG from spec"""
        template_path = self.base_dir / "app" / "dag_template.py.j2"
        
        with open(template_path, 'r') as f:
            template = Template(f.read())
        
        return template.render(spec=spec)
    
    def generate_sql(self, spec: PipelineSpec) -> str:
        """Generate SQL transformation file"""
        
        # Basic SQL template with deduplication logic
        sql_template = """-- Generated SQL for {{ spec.pipeline_name }}
-- Transform: {{ spec.transform.sql_file }}

WITH base AS (
  -- Source data (will be dynamically populated by extract task)
  SELECT * FROM source_data
),
{% if spec.transform.params.get('dedupe_key') %}
deduped AS (
  SELECT *
  FROM (
    SELECT *, 
           ROW_NUMBER() OVER (
             PARTITION BY {{ spec.transform.params.dedupe_key }} 
             ORDER BY {{ spec.transform.params.get('latest_col', 'created_at') }} DESC
           ) AS rn
    FROM base
  ) t
  WHERE rn = 1
),
final AS (
  SELECT * EXCLUDE (rn)
  FROM deduped
)
{% else %}
final AS (
  SELECT * FROM base
)
{% endif %}
SELECT *, 
       CURRENT_DATE AS ingest_date,
       CURRENT_TIMESTAMP AS processed_at
FROM final;
"""
        
        template = Template(sql_template)
        return template.render(spec=spec)
    
    def generate_test(self, spec: PipelineSpec) -> str:
        """Generate basic smoke test"""
        
        test_template = """# Generated tests for {{ spec.pipeline_name }}
import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.etl_ops import select_source, run_sql
from app.checks import assert_min_rows
from app.spec_schema import PipelineSpec

def test_{{ spec.pipeline_name }}_spec_validation():
    \"\"\"Test that the spec loads and validates correctly\"\"\"
    spec_path = "specs/{{ spec.pipeline_name }}.json"
    assert os.path.exists(spec_path), f"Spec file not found: {spec_path}"
    
    with open(spec_path, 'r') as f:
        spec_dict = json.load(f)
    
    # Should not raise validation error
    spec = PipelineSpec(**spec_dict)
    assert spec.pipeline_name == "{{ spec.pipeline_name }}"

def test_{{ spec.pipeline_name }}_source_selection():
    \"\"\"Test source selection logic\"\"\"
    # Mock spec
    spec = {
        'sources': {{ spec.sources | tojson }},
        'source_routing': {{ spec.source_routing | tojson }}
    }
    
    with patch.dict(os.environ, {'ENV': 'dev'}):
        source = select_source(spec, 'dev')
        assert source in [s['name'] for s in spec['sources']]

def test_{{ spec.pipeline_name }}_min_rows_check():
    \"\"\"Test minimum rows validation\"\"\"
    # Mock DuckDB relation with test data
    mock_rel = Mock()
    mock_rel.execute.return_value.fetchall.return_value = [{{ spec.checks.min_rows + 1 }}]
    
    # Should not raise
    assert_min_rows(mock_rel, {{ spec.checks.min_rows }})
"""
        
        template = Template(test_template)
        return template.render(spec=spec)
    
    def save_files(self, spec: PipelineSpec, dag_code: str, sql_code: str, test_code: str):
        """Save generated files to appropriate directories"""
        
        # Create directories if they don't exist
        for dir_name in ['dags', 'sql', 'tests', 'configs']:
            (self.base_dir / dir_name).mkdir(parents=True, exist_ok=True)
        
        # Save DAG file
        dag_path = self.base_dir / "dags" / f"{spec.pipeline_name}.py"
        with open(dag_path, 'w') as f:
            f.write(dag_code)
        print(f"✓ Generated DAG: {dag_path}")
        
        # Save SQL file  
        sql_path = self.base_dir / "sql" / spec.transform.sql_file
        with open(sql_path, 'w') as f:
            f.write(sql_code)
        print(f"✓ Generated SQL: {sql_path}")
        
        # Save test file
        test_path = self.base_dir / "tests" / f"test_{spec.pipeline_name}.py"
        with open(test_path, 'w') as f:
            f.write(test_code)
        print(f"✓ Generated test: {test_path}")
    
    def generate_pipeline(self, spec_path: str):
        """Main generation workflow"""
        spec = self.load_spec(spec_path)
        
        dag_code = self.generate_dag(spec)
        sql_code = self.generate_sql(spec)  
        test_code = self.generate_test(spec)
        
        self.save_files(spec, dag_code, sql_code, test_code)
        
        print(f"✓ Pipeline '{spec.pipeline_name}' generated successfully")

def main():
    parser = argparse.ArgumentParser(description="Generate Airflow pipeline from spec")
    parser.add_argument("--spec", required=True, help="Path to pipeline spec JSON file")
    
    args = parser.parse_args()
    
    generator = PipelineGenerator()
    
    try:
        generator.generate_pipeline(args.spec)
    except Exception as e:
        print(f"✗ Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()

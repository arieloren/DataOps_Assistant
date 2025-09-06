import argparse
import json
from pathlib import Path
from typing import Dict, List

from .router import LLMRouter
from .spec_schema import PipelineSpec

class RequestParser:
    def __init__(self):
        self.router = LLMRouter(mode="closed")
        
    def parse_request(self, prompt: str) -> Dict:
        """Convert natural language prompt to pipeline spec"""
        
        system_prompt = """
You are an expert ETL pipeline generator. Convert the user's natural language request into a JSON specification.

The user will describe:
- Data sources (CSV files, REST APIs, databases)
- Transform requirements (join, dedupe, etc.)
- Schedule (daily, weekly, on-demand)
- Output formats (SQLite, Parquet)

Generate a JSON spec following this exact schema:

{
  "pipeline_name": "descriptive_name",
  "schedule": {
    "on_demand": true,
    "daily_02": false, 
    "weekly_mon_06": false
  },
  "profile": "dev",
  "sources": [
    {
      "type": "file",
      "name": "source_name",
      "path": "./datasets/filename_*.csv",
      "format": "csv",
      "options": {"header": true},
      "enabled_if": "env == 'dev'"
    },
    {
      "type": "rest", 
      "name": "api_name",
      "base_url": "http://localhost:3000",
      "path": "/v1/endpoint",
      "pagination": {"param": "page", "size": 100},
      "enabled_if": "true"
    }
  ],
  "source_routing": {
    "primary": ["source1", "source2"]
  },
  "transform": {
    "sql_file": "pipeline_name.sql",
    "params": {"dedupe_key": "id", "latest_col": "updated_at"}
  },
  "outputs": {
    "sqlite": {"path": "data/sqlite/warehouse.db", "mode": "merge", "table": "table_name"},
    "parquet": {"dir": "data/parquet/table_name/", "partition_by": ["ingest_date"]}
  },
  "checks": {
    "min_rows": 1,
    "null_ratio": {"columns": ["id"], "max": 0.0}
  }
}

Rules:
- Infer source types from context (CSV=file, API=rest, database=postgres)
- Set enabled_if: file sources use "env == 'dev'", rest/postgres use "true" or "env == 'prod'"
- Parse schedules: "daily 2am" → daily_02=true, "weekly Monday 6am" → weekly_mon_06=true
- Always include on_demand=true
- Generate descriptive pipeline_name from the request
- Include reasonable defaults for all fields
- Return ONLY the JSON, no other text

User request: """ + prompt

        response = self.router.generate(system_prompt)
        
        # Clean response to extract JSON
        response = response.strip()
        if response.startswith('```json'):
            response = response[7:]
        if response.endswith('```'):
            response = response[:-3]
        
        try:
            spec_dict = json.loads(response)
            return spec_dict
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse LLM response as JSON: {e}\nResponse: {response}")

    def validate_and_save_spec(self, spec_dict: Dict, output_dir: str) -> str:
        """Validate spec and save to file"""
        # Validate with Pydantic
        spec = PipelineSpec(**spec_dict)
        
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Save to file
        output_path = Path(output_dir) / f"{spec.pipeline_name}.json"
        with open(output_path, 'w') as f:
            json.dump(spec_dict, f, indent=2)
        
        return str(output_path)

def main():
    parser = argparse.ArgumentParser(description="Convert natural language to ETL pipeline spec")
    parser.add_argument("--prompt", required=True, help="Natural language pipeline description")
    parser.add_argument("--output-dir", default="specs", help="Output directory for spec files")
    
    args = parser.parse_args()
    
    request_parser = RequestParser()
    
    try:
        spec_dict = request_parser.parse_request(args.prompt)
        output_path = request_parser.validate_and_save_spec(spec_dict, args.output_dir)
        print(f"✓ Pipeline spec saved to: {output_path}")
        print(f"✓ Pipeline name: {spec_dict['pipeline_name']}")
    except Exception as e:
        print(f"✗ Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()

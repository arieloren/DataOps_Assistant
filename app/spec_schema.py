from pydantic import BaseModel, Field, validator
from typing import Dict, List, Optional, Any, Union
from enum import Enum

class SourceType(str, Enum):
    FILE = "file"
    REST = "rest" 
    POSTGRES = "postgres"

class FileSource(BaseModel):
    type: str = Field(..., regex="^file$")
    name: str
    path: str
    format: str = "csv"
    options: Dict[str, Any] = {}
    enabled_if: str = "true"

class PaginationConfig(BaseModel):
    param: str = "page"
    size: int = 100

class SinceConfig(BaseModel):
    field: str
    mode: str = "incremental"

class RestSource(BaseModel):
    type: str = Field(..., regex="^rest$")
    name: str
    base_url: str
    path: str
    pagination: Optional[PaginationConfig] = None
    since: Optional[SinceConfig] = None
    enabled_if: str = "true"

class PostgresSource(BaseModel):
    type: str = Field(..., regex="^postgres$")
    name: str
    conn_id: str
    table: str
    mode: str = "incremental"
    updated_at: Optional[str] = None
    enabled_if: str = "true"

class ScheduleConfig(BaseModel):
    on_demand: bool = True
    daily_02: bool = False
    weekly_mon_06: bool = False

class TransformConfig(BaseModel):
    sql_file: str
    params: Dict[str, str] = {}

class SqliteOutput(BaseModel):
    path: str
    mode: str = "merge"
    table: str

class ParquetOutput(BaseModel):
    dir: str
    partition_by: List[str] = ["ingest_date"]

class OutputsConfig(BaseModel):
    sqlite: Optional[SqliteOutput] = None
    parquet: Optional[ParquetOutput] = None

class NullRatioCheck(BaseModel):
    columns: List[str]
    max: float = 0.0

class ChecksConfig(BaseModel):
    min_rows: int = 1
    null_ratio: Optional[NullRatioCheck] = None

class SourceRoutingConfig(BaseModel):
    primary: List[str]

class PipelineSpec(BaseModel):
    pipeline_name: str
    schedule: ScheduleConfig
    profile: str = "dev"
    sources: List[Union[FileSource, RestSource, PostgresSource]]
    source_routing: SourceRoutingConfig
    transform: TransformConfig
    outputs: OutputsConfig
    checks: ChecksConfig

    @validator('sources')
    def validate_sources(cls, v):
        if not v:
            raise ValueError("At least one source is required")
        return v

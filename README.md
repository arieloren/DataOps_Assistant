# ETL Pipeline MVP - Phase A

Convert natural language requests into working Airflow ETL pipelines.

## Quick Start

1. **Setup Environment**
   ```bash
   # Copy and edit environment file
   cp .env.example .env
   # Add your OPENAI_API_KEY
   
   # Install dependencies
   make setup
   ```

2. **Start Airflow**
   ```bash
   make dev-up
   ```
   Visit http://localhost:8080 (airflow/airflow)

3. **Generate Pipeline**
   ```bash
   # Parse natural language to spec
   python -m app.a1_parser --prompt "Load orders CSV, enrich with items API, output parquet and sqlite"
   
   # Generate Airflow DAG
   python -m app.a2_generator --spec specs/orders_enriched.json
   ```

4. **Run Pipeline**
   - Go to Airflow UI
   - Find your generated DAG
   - Trigger manually
   - Check outputs in `data/` directory

## Architecture

- **A1 Parser**: Natural language → JSON spec
- **A2 Generator**: JSON spec → Airflow DAG + SQL
- **Runtime**: Local Airflow + DuckDB transforms
- **Outputs**: SQLite databases + Parquet files
- **Catalog**: Run metadata tracking

## File Structure

- `specs/` - Generated pipeline specifications
- `dags/` - Generated Airflow DAGs  
- `sql/` - Generated SQL transformations
- `data/` - Pipeline outputs (SQLite, Parquet)
- `app/` - Core pipeline generation code

## Example Usage

```bash
# Generate e-commerce pipeline
make parse PROMPT="Daily ETL: load orders CSV, join with products API, dedupe by order_id, save to warehouse"

# Generate the DAG
make generate SPEC=specs/daily_orders_etl.json

# Check results
ls data/sqlite/ data/parquet/
```

## Testing

```bash
make test    # Run unit tests
make lint    # Code formatting
make clean   # Clear outputs
```

## Phase B Preview

Next phase will add:
- AWS S3/RDS sources
- EC2 Airflow deployment  
- Production scheduling
- Monitoring & alerts

This completes the full Phase A MVP implementation! The system provides:

✅ **A1 Parser** - Converts natural language to validated JSON specs
✅ **A2 Generator** - Creates runnable Airflow DAGs from specs  
✅ **Local Airflow Stack** - Docker Compose with all dependencies
✅ **Data Connectors** - File, REST API, and PostgreSQL support
✅ **DuckDB Transforms** - Fast SQL processing with deduplication
✅ **Multiple Outputs** - SQLite and Parquet with partitioning
✅ **Quality Checks** - Row counts and null ratio validation
✅ **Run Catalog** - SQLite-based execution tracking
✅ **Clean Architecture** - Ready for Phase B AWS integration

You can now run the complete end-to-end workflow from natural language to working data pipelines!

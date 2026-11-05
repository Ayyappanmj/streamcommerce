# Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `MissingDependencyError: duckdb is not installed` | `pip install -r requirements.txt` inside the venv. The `process` step works without duckdb; `load_warehouse` needs it. |
| `pyarrow is required for PROCESSED_FORMAT=parquet` | Install pyarrow, or set `PROCESSED_FORMAT=csv` in `.env`. |
| `Cannot connect to PostgreSQL` | `docker compose up -d postgres`, check `POSTGRES_PASSWORD` in `.env`, or use `METADATA_BACKEND=sqlite`. |
| Compose: `set POSTGRES_PASSWORD in .env` | Copy `.env.example` to `.env` and set a password. |
| PySpark: `JAVA_GATEWAY_EXITED` / Java not found | Install JDK 17 and ensure `java -version` works in the same PowerShell session. On Windows Spark may also need `HADOOP_HOME` with winutils for some write operations (this project only reads CSV with Spark). Use `--engine pandas` to avoid Spark. |
| Kafka consumer sees no messages | Start `docker compose --profile streaming up -d`; host port is `localhost:19092` (`KAFKA_BOOTSTRAP_SERVERS`). The consumer exits after `--idle-timeout` seconds without messages. |
| Same file "unchanged file skipped" | By design (checksum already processed). Use `--force` to reprocess. |
| `Schema ... missing required columns` | The file failed validation; its manifest row is `failed` with details. Fix the file and rerun. |
| dbt: `Dependency Error` or table missing | Run `load_warehouse` first (dbt reads `raw.*`). Do not keep the DuckDB file open in another tool (single writer). |
| DuckDB `database is locked` | Close the dashboard / DuckDB CLI holding the file, or use read-only connections. |
| Streamlit shows "Warehouse not found" | Run the batch pipeline with `load_warehouse` first. |
| PowerShell: running scripts is disabled | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`, then run the script. |
| Airflow import error for `src` | Set `STREAMCOMMERCE_HOME` / `PYTHONPATH` to the project root (done in `docker/Dockerfile.airflow`). |

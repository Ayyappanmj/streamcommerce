# Optional Snowflake integration

**Disabled by default.** Nothing in the core pipeline imports this package. To keep Snowflake off, simply do not
run `python -m src.snowflake.loader` and do not install `requirements-snowflake.txt`.

## 1. Install the optional dependency
```powershell
pip install -r requirements-snowflake.txt
```

## 2. Create objects (run once as an admin; adjust names)
```sql
CREATE WAREHOUSE IF NOT EXISTS STREAMCOMMERCE_WH WITH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE IF NOT EXISTS STREAMCOMMERCE;
CREATE SCHEMA IF NOT EXISTS STREAMCOMMERCE.ANALYTICS;
CREATE ROLE IF NOT EXISTS STREAMCOMMERCE_LOADER;
GRANT USAGE, OPERATE ON WAREHOUSE STREAMCOMMERCE_WH TO ROLE STREAMCOMMERCE_LOADER;
GRANT USAGE ON DATABASE STREAMCOMMERCE TO ROLE STREAMCOMMERCE_LOADER;
GRANT USAGE, CREATE TABLE ON SCHEMA STREAMCOMMERCE.ANALYTICS TO ROLE STREAMCOMMERCE_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA STREAMCOMMERCE.ANALYTICS TO ROLE STREAMCOMMERCE_LOADER;
GRANT ROLE STREAMCOMMERCE_LOADER TO USER <your_user>;
```

## 3. Key-pair authentication
Generate an RSA key pair, assign the public key with `ALTER USER <u> SET RSA_PUBLIC_KEY='...'`, keep the private key
file **outside** the repository, then set the environment variables from `.env.example`
(`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_SCHEMA`, `SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT`, `SNOWFLAKE_PRIVATE_KEY_PATH`,
optional `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`).

## 4. Run
```powershell
python -m src.snowflake.loader --dry-run   # prints the plan, contacts nothing
python -m src.snowflake.loader             # stages with write_pandas, then MERGEs into the target tables
```

## Behaviour and safety
* Target tables: `CREATE TABLE IF NOT EXISTS`, then `MERGE` (idempotent upsert). No `DROP`/`CREATE OR REPLACE` on targets.
* Only the `<TABLE>_STG` staging tables are overwritten on each run.
* SQL identifiers come from fixed, validated constants; row values travel through `write_pandas`, not SQL text.
* Credentials are never logged.

## Cost note
Snowflake bills compute credits while the warehouse runs (XSMALL, auto-suspend 60 s here) plus storage. A small
demo load is expected to be cheap but **this project has not measured the cost**; check your account's usage views.
Always set `AUTO_SUSPEND` and consider a resource monitor.

**Status:** written against the documented connector API but NOT executed against a real Snowflake account.

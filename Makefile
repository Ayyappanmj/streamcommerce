# Convenience for Linux/macOS/WSL. Windows users: use scripts\*.ps1 (see README).
PY ?= python
.PHONY: install test lint batch stream dashboard dbt
install: ; $(PY) -m pip install -r requirements-dev.txt
test: ; $(PY) -m pytest -m "not external"
lint: ; $(PY) -m ruff check .
batch: ; $(PY) -m src.batch_pipeline --generate --with-dbt
stream: ; $(PY) -m src.ingestion.streaming_demo --transport kafka --duration 30
dashboard: ; streamlit run app/dashboard.py
dbt: ; cd dbt_project && dbt build --profiles-dir .

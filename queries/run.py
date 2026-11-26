"""Run a DuckDB query file against the local copies made by infra/fetch_local.sh.

Usage (from the repo root):
    python queries/run.py queries/02_rise_above_category.sql
"""
import sys
import duckdb

sql = open(sys.argv[1]).read()
print(duckdb.sql(sql))

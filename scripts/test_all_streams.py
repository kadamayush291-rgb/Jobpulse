import os
import sys
import json

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from kafka_stream_scheduler import get_kafka_scheduler
from duckdb_storage import DuckDBStorage

scheduler = get_kafka_scheduler(interval_seconds=60)
print(f"Total Datasets in Queue: {len(scheduler.datasets)}")
print("Queue:", scheduler.datasets)

results = []
for i in range(len(scheduler.datasets)):
    res = scheduler.stream_dataset()
    results.append(res)
    print(f"[{i+1}/{len(scheduler.datasets)}] Streamed: {res['dataset']} | Records: {res['records_ingested']} | Quality: {res['quality_score']}% | DuckDB: {res['duckdb_olap_latency_ms']}ms | Next: {res['next_dataset']}")

print("\n--- FINAL DUCKDB WAREHOUSE STATE ---")
duckdb_wh = DuckDBStorage()
stats = duckdb_wh.get_stats()
print(json.dumps(stats, indent=2))

# Verify OLAP query on Indian Tech Hubs
olap_sample = duckdb_wh.execute_custom_query("SELECT city, country, COUNT(*) as jobs, ROUND(AVG(salary_avg), 0) as avg_compensation FROM fact_jobs GROUP BY city, country ORDER BY jobs DESC LIMIT 10")
print("\n--- TOP TECH HUBS IN DUCKDB ---")
for r in olap_sample['data']:
    print(f"  {r['city']} ({r['country']}): {r['jobs']} postings, avg: {r['avg_compensation']}")

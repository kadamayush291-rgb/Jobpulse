"""
JobPulse - Python DuckDB Analytical Storage & OLAP Query Engine
Provides high-performance columnar storage and executes advanced OLAP analytical queries
(rollups, window functions, percentiles, co-occurrence matrices, cross-tabs)
with sub-millisecond execution times.
"""
import os
import sys
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobPulse.DuckDB")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
DUCKDB_PATH = os.path.join(DATA_DIR, "jobpulse_olap.duckdb")


class DuckDBStorage:
    """
    DuckDB Columnar Analytical Warehouse & OLAP Engine.
    """
    def __init__(self, db_path: str = DUCKDB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_schema()

    def get_connection(self):
        return duckdb.connect(self.db_path)

    def init_schema(self):
        """Creates fact, dimension, audit, and streaming event tables in DuckDB."""
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_jobs (
                    job_id INTEGER PRIMARY KEY,
                    job_title VARCHAR,
                    company VARCHAR,
                    city VARCHAR,
                    state VARCHAR,
                    country VARCHAR,
                    salary_min DOUBLE,
                    salary_max DOUBLE,
                    salary_avg DOUBLE,
                    salary_currency VARCHAR,
                    job_type VARCHAR,
                    experience_level VARCHAR,
                    posted_date DATE,
                    source VARCHAR,
                    description VARCHAR,
                    quality_score DOUBLE,
                    ingested_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS dim_skills (
                    skill_id INTEGER PRIMARY KEY,
                    job_id INTEGER,
                    skill_name VARCHAR,
                    skill_category VARCHAR
                );

                CREATE TABLE IF NOT EXISTS kafka_events_log (
                    event_id VARCHAR,
                    topic VARCHAR,
                    partition INTEGER,
                    offset_num BIGINT,
                    message_key VARCHAR,
                    event_timestamp VARCHAR,
                    consumed_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS data_quality_audit (
                    audit_id INTEGER PRIMARY KEY,
                    rule_id VARCHAR,
                    rule_name VARCHAR,
                    category VARCHAR,
                    status VARCHAR,
                    failed_count INTEGER,
                    total_checked INTEGER,
                    details VARCHAR,
                    audited_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS olap_query_history (
                    query_id INTEGER PRIMARY KEY,
                    query_name VARCHAR,
                    sql_query VARCHAR,
                    execution_time_ms DOUBLE,
                    row_count INTEGER,
                    executed_at TIMESTAMP
                );
            """)
        logger.info(f"DuckDB schema initialized at {self.db_path}")

    def load_clean_data(
        self,
        fact_df: pd.DataFrame,
        skills_df: pd.DataFrame,
        quality_report: Optional[Dict[str, Any]] = None,
        kafka_messages: Optional[List[Dict[str, Any]]] = None,
        append: bool = True
    ) -> Dict[str, Any]:
        """
        Loads cleaned fact and dimension DataFrames into DuckDB using fast columnar ingestion.
        When append=True, appends records while preventing duplicates on (job_title, company, city).
        """
        start_time = time.time()
        with self.get_connection() as conn:
            # 1. Load fact_jobs
            if not append:
                conn.execute("DELETE FROM fact_jobs")
            
            if not fact_df.empty:
                # Re-index job_id if appending to existing table
                if append:
                    max_id_row = conn.execute("SELECT COALESCE(MAX(job_id), 0) FROM fact_jobs").fetchone()
                    offset = max_id_row[0] if max_id_row else 0
                    if offset > 0:
                        fact_df = fact_df.copy()
                        fact_df['job_id'] = fact_df['job_id'] + offset
                        if not skills_df.empty:
                            skills_df = skills_df.copy()
                            skills_df['job_id'] = skills_df['job_id'] + offset

                conn.register("fact_df_view", fact_df)
                if append:
                    conn.execute("""
                        INSERT INTO fact_jobs BY NAME 
                        SELECT * FROM fact_df_view 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM fact_jobs f 
                            WHERE f.job_title = fact_df_view.job_title 
                              AND f.company = fact_df_view.company 
                              AND f.city = fact_df_view.city
                        )
                    """)
                else:
                    conn.execute("INSERT INTO fact_jobs BY NAME SELECT * FROM fact_df_view")
                conn.unregister("fact_df_view")

            # 2. Load dim_skills
            if not append:
                conn.execute("DELETE FROM dim_skills")
            if not skills_df.empty:
                if append:
                    max_sid_row = conn.execute("SELECT COALESCE(MAX(skill_id), 0) FROM dim_skills").fetchone()
                    s_offset = max_sid_row[0] if max_sid_row else 0
                    if s_offset > 0:
                        skills_df = skills_df.copy()
                        skills_df['skill_id'] = skills_df['skill_id'] + s_offset

                conn.register("skills_df_view", skills_df)
                if append:
                    conn.execute("""
                        INSERT INTO dim_skills BY NAME 
                        SELECT * FROM skills_df_view 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM dim_skills s 
                            WHERE s.job_id = skills_df_view.job_id 
                              AND s.skill_name = skills_df_view.skill_name
                        )
                    """)
                else:
                    conn.execute("INSERT INTO dim_skills BY NAME SELECT * FROM skills_df_view")
                conn.unregister("skills_df_view")

            # 3. Load quality audit trail
            if quality_report and "audit_trail" in quality_report:
                if not append:
                    conn.execute("DELETE FROM data_quality_audit")
                max_aid_row = conn.execute("SELECT COALESCE(MAX(audit_id), 0) FROM data_quality_audit").fetchone()
                a_offset = (max_aid_row[0] if max_aid_row else 0) if append else 0

                audit_records = []
                for idx, r in enumerate(quality_report["audit_trail"]):
                    audit_records.append({
                        "audit_id": a_offset + idx + 1,
                        "rule_id": r.get("rule_id"),
                        "rule_name": r.get("rule_name"),
                        "category": r.get("category"),
                        "status": r.get("status"),
                        "failed_count": int(r.get("failed_count", 0)),
                        "total_checked": int(r.get("total_checked", 0)),
                        "details": r.get("details"),
                        "audited_at": datetime.now()
                    })
                if audit_records:
                    audit_df = pd.DataFrame(audit_records)
                    conn.register("audit_df_view", audit_df)
                    conn.execute("INSERT INTO data_quality_audit BY NAME SELECT * FROM audit_df_view")
                    conn.unregister("audit_df_view")

            # 4. Load Kafka event tracking
            if kafka_messages:
                k_records = []
                for idx, msg in enumerate(kafka_messages):
                    val = msg.get("value", {})
                    k_records.append({
                        "event_id": val.get("event_id", f"evt_{int(time.time()*1000)}_{idx}"),
                        "topic": msg.get("topic", "jobpulse-raw-jobs"),
                        "partition": int(msg.get("partition", 0)),
                        "offset_num": int(msg.get("offset", idx)),
                        "message_key": str(msg.get("key", "")),
                        "event_timestamp": str(msg.get("timestamp", "")),
                        "consumed_at": datetime.now()
                    })
                if k_records:
                    k_df = pd.DataFrame(k_records)
                    conn.register("k_df_view", k_df)
                    conn.execute("INSERT INTO kafka_events_log BY NAME SELECT * FROM k_df_view")
                    conn.unregister("k_df_view")

        load_duration = (time.time() - start_time) * 1000.0
        logger.info(f"Loaded {len(fact_df)} jobs and {len(skills_df)} skills into DuckDB in {load_duration:.2f}ms.")

        return {
            "status": "SUCCESS",
            "fact_jobs_stored": len(fact_df),
            "dim_skills_stored": len(skills_df),
            "quality_audit_entries": len(quality_report.get("audit_trail", [])) if quality_report else 0,
            "kafka_events_logged": len(kafka_messages) if kafka_messages else 0,
            "load_time_ms": round(load_duration, 2)
        }

    def execute_custom_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Executes an arbitrary SQL query on DuckDB and records execution latency in ms.
        """
        start = time.perf_counter()
        with self.get_connection() as conn:
            try:
                rel = conn.execute(sql_query)
                df = rel.df()
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                
                # Replace NaNs for clean JSON serialization
                df_clean = df.fillna(value="").replace({float('nan'): None})
                records = df_clean.to_dict(orient="records")

                return {
                    "success": True,
                    "columns": list(df.columns),
                    "data": records,
                    "row_count": len(df),
                    "execution_time_ms": round(elapsed_ms, 3)
                }
            except Exception as e:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                return {
                    "success": False,
                    "error": str(e),
                    "columns": [],
                    "data": [],
                    "row_count": 0,
                    "execution_time_ms": round(elapsed_ms, 3)
                }

    @staticmethod
    def get_olap_queries() -> Dict[str, Dict[str, str]]:
        """
        Returns catalog of predefined OLAP analytical queries demonstrating DuckDB's advanced SQL features.
        """
        return {
            "salary_percentiles": {
                "title": "Salary Percentiles by Experience & Currency (P25, P50, P75, P90)",
                "category": "Compensation Analytics",
                "description": "Computes exact parametric percentiles and Interquartile Range (IQR) for salary distributions using DuckDB's QUANTILE_CONT analytical function across currencies.",
                "sql": """
                    SELECT 
                        experience_level,
                        COALESCE(salary_currency, 'INR') AS currency,
                        COUNT(*) AS job_count,
                        ROUND(AVG(salary_avg), 2) AS mean_salary,
                        ROUND(QUANTILE_CONT(salary_avg, 0.25), 2) AS p25_salary,
                        ROUND(QUANTILE_CONT(salary_avg, 0.50), 2) AS median_salary,
                        ROUND(QUANTILE_CONT(salary_avg, 0.75), 2) AS p75_salary,
                        ROUND(QUANTILE_CONT(salary_avg, 0.90), 2) AS p90_salary,
                        ROUND(QUANTILE_CONT(salary_avg, 0.75) - QUANTILE_CONT(salary_avg, 0.25), 2) AS iqr_spread
                    FROM fact_jobs
                    WHERE salary_avg IS NOT NULL AND experience_level IS NOT NULL
                    GROUP BY experience_level, salary_currency
                    ORDER BY currency, median_salary DESC;
                """
            },
            "geo_rollup": {
                "title": "Hierarchical Geo Aggregation (ROLLUP: Country -> State -> City)",
                "category": "Geographic Analytics",
                "description": "Generates multi-tier subtotal and grand-total hierarchical aggregations across country, state, and city levels using DuckDB's GROUP BY ROLLUP.",
                "sql": """
                    SELECT 
                        COALESCE(country, 'TOTAL (ALL)') AS country,
                        COALESCE(state, 'ALL STATES') AS state,
                        COALESCE(city, 'ALL CITIES') AS city,
                        COUNT(*) AS total_postings,
                        ROUND(AVG(salary_avg), 2) AS avg_salary,
                        ROUND(MIN(salary_min), 2) AS min_salary,
                        ROUND(MAX(salary_max), 2) AS max_salary
                    FROM fact_jobs
                    WHERE salary_avg IS NOT NULL
                    GROUP BY ROLLUP (country, state, city)
                    ORDER BY country, state NULLS FIRST, total_postings DESC
                    LIMIT 20;
                """
            },
            "skill_market_ranking": {
                "title": "Skill Market Value & Category Window Ranking",
                "category": "Skill Analytics",
                "description": "Ranks top in-demand skills within each technical domain using DENSE_RANK() and computes salary premium relative to the domain average.",
                "sql": """
                    WITH skill_metrics AS (
                        SELECT 
                            s.skill_category,
                            s.skill_name,
                            COUNT(DISTINCT f.job_id) AS demand_frequency,
                            ROUND(AVG(f.salary_avg), 2) AS avg_offered_salary,
                            ROUND(MEDIAN(f.salary_avg), 2) AS median_salary
                        FROM dim_skills s
                        JOIN fact_jobs f ON s.job_id = f.job_id
                        WHERE f.salary_avg IS NOT NULL
                        GROUP BY s.skill_category, s.skill_name
                        HAVING COUNT(DISTINCT f.job_id) >= 2
                    )
                    SELECT 
                        skill_category,
                        skill_name,
                        demand_frequency,
                        avg_offered_salary,
                        median_salary,
                        DENSE_RANK() OVER (PARTITION BY skill_category ORDER BY demand_frequency DESC, avg_offered_salary DESC) as category_rank,
                        ROUND(avg_offered_salary - AVG(avg_offered_salary) OVER (PARTITION BY skill_category), 2) as diff_from_cat_avg
                    FROM skill_metrics
                    ORDER BY skill_category, category_rank
                    LIMIT 25;
                """
            },
            "skill_co_occurrence": {
                "title": "Skill Co-Occurrence Association Network",
                "category": "Market Intelligence",
                "description": "Performs an analytical self-join on dim_skills to uncover frequently co-occurring skill pairs and the average compensation for candidates possessing both.",
                "sql": """
                    SELECT 
                        s1.skill_name AS primary_skill,
                        s2.skill_name AS paired_skill,
                        COUNT(*) AS pair_frequency,
                        ROUND(AVG(f.salary_avg), 2) AS avg_salary_with_pair
                    FROM dim_skills s1
                    JOIN dim_skills s2 ON s1.job_id = s2.job_id AND s1.skill_name < s2.skill_name
                    JOIN fact_jobs f ON s1.job_id = f.job_id
                    GROUP BY s1.skill_name, s2.skill_name
                    HAVING COUNT(*) >= 2
                    ORDER BY pair_frequency DESC, avg_salary_with_pair DESC
                    LIMIT 15;
                """
            },
            "company_cume_dist": {
                "title": "Corporate Compensation Cumulative Distribution (CUME_DIST & PERCENT_RANK)",
                "category": "Enterprise Benchmark",
                "description": "Evaluates company salary tiers using cumulative distribution and percentile rank window functions.",
                "sql": """
                    SELECT 
                        company,
                        COUNT(*) AS total_openings,
                        ROUND(AVG(salary_avg), 2) AS avg_salary,
                        ROUND(CUME_DIST() OVER (ORDER BY AVG(salary_avg)), 4) AS salary_cume_dist,
                        ROUND(PERCENT_RANK() OVER (ORDER BY AVG(salary_avg)), 4) AS salary_percent_rank
                    FROM fact_jobs
                    WHERE salary_avg IS NOT NULL
                    GROUP BY company
                    HAVING COUNT(*) >= 1
                    ORDER BY avg_salary DESC
                    LIMIT 15;
                """
            },
            "crosstab_category_experience": {
                "title": "Cross-Tabulation Matrix: Skill Category Demand by Experience Level",
                "category": "Matrix Analysis",
                "description": "Pivots skill category counts across experience levels to visualize hiring focus.",
                "sql": """
                    SELECT 
                        f.experience_level,
                        COUNT(CASE WHEN s.skill_category = 'Programming' THEN 1 END) AS programming_skills,
                        COUNT(CASE WHEN s.skill_category = 'Cloud' THEN 1 END) AS cloud_skills,
                        COUNT(CASE WHEN s.skill_category = 'Data/DB' THEN 1 END) AS data_skills,
                        COUNT(CASE WHEN s.skill_category = 'ML/AI' THEN 1 END) AS ml_ai_skills,
                        COUNT(CASE WHEN s.skill_category = 'DevOps' THEN 1 END) AS devops_skills,
                        COUNT(CASE WHEN s.skill_category = 'Visualization' THEN 1 END) AS viz_skills,
                        COUNT(*) AS total_skill_mentions
                    FROM fact_jobs f
                    JOIN dim_skills s ON f.job_id = s.job_id
                    WHERE f.experience_level IS NOT NULL
                    GROUP BY f.experience_level
                    ORDER BY total_skill_mentions DESC;
                """
            }
        }

    def run_olap_suite(self) -> Dict[str, Any]:
        """
        Executes all defined OLAP queries and returns comprehensive performance benchmark results.
        """
        queries = self.get_olap_queries()
        results = {}
        total_time_ms = 0.0

        for key, q_meta in queries.items():
            exec_res = self.execute_custom_query(q_meta["sql"])
            exec_time = exec_res["execution_time_ms"]
            total_time_ms += exec_time
            results[key] = {
                "title": q_meta["title"],
                "category": q_meta["category"],
                "description": q_meta["description"],
                "sql": q_meta["sql"].strip(),
                "execution_time_ms": exec_time,
                "row_count": exec_res["row_count"],
                "columns": exec_res["columns"],
                "data": exec_res["data"]
            }

        return {
            "total_queries_executed": len(queries),
            "cumulative_execution_time_ms": round(total_time_ms, 3),
            "average_latency_ms": round(total_time_ms / len(queries), 3) if queries else 0.0,
            "queries": results
        }

    def get_stats(self) -> Dict[str, Any]:
        """Returns statistics on DuckDB database, row counts, and table sizes."""
        stats = {}
        file_size_kb = round(os.path.getsize(self.db_path) / 1024.0, 2) if os.path.exists(self.db_path) else 0

        with self.get_connection() as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            counts = {}
            for t in table_names:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                counts[t] = cnt

        return {
            "database_engine": "DuckDB Columnar In-Process OLAP",
            "database_path": self.db_path,
            "file_size_kb": file_size_kb,
            "tables": counts,
            "total_records": sum(counts.values())
        }


if __name__ == "__main__":
    db = DuckDBStorage()
    suite = db.run_olap_suite()
    print(f"Executed {suite['total_queries_executed']} OLAP queries in {suite['cumulative_execution_time_ms']}ms (Avg: {suite['average_latency_ms']}ms)")

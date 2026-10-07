"""
JobPulse - 1-Minute Multi-Dataset Streaming Pipeline Runner
============================================================
This script:
  1. Generates all 16 diverse streaming datasets (if not already present)
  2. Starts the Kafka Stream Scheduler that automatically:
     - Picks the next dataset from the rotation queue every 60 seconds
     - Streams it through: CSV -> Kafka Producer -> Kafka Consumer
                          -> Spark Quality Engine -> DuckDB OLAP
     - Logs per-cycle results to console and scheduler state JSON
  3. Runs indefinitely until Ctrl+C is pressed

Usage:
    python run_streaming_pipeline.py              # start with defaults
    python run_streaming_pipeline.py --now        # stream once immediately, then schedule
    python run_streaming_pipeline.py --regenerate # regenerate datasets before starting
    python run_streaming_pipeline.py --interval 30 # run every 30 seconds instead of 60
"""
import os
import sys
import json
import time
import signal
import logging
import argparse
import io

# Force UTF-8 output on Windows to avoid cp1252 UnicodeEncodeError
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
from datetime import datetime

# Ensure scripts directory is on path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("JobPulse.StreamRunner")

SEPARATOR = "=" * 72
DIV       = "-" * 60


def banner():
    print(f"""
{SEPARATOR}
  JobPulse - Multi-Dataset 1-Minute Kafka Streaming Pipeline
  Datasets  : 16 diverse domains (US FinTech, India GCC, EU Cloud, AI/LLM...)
  Pipeline  : CSV -> Kafka -> Spark Quality Engine -> DuckDB OLAP
  Schedule  : Rotates through all datasets, 1 dataset per minute
{SEPARATOR}
""")


def generate_datasets(regenerate: bool = False):
    """Run the dataset generator to populate streaming_datasets/."""
    logger.info("Checking / generating streaming datasets ...")
    from generate_streaming_datasets import generate_all_datasets, list_all_streaming_datasets
    paths = generate_all_datasets(regenerate=regenerate)
    existing = list_all_streaming_datasets()
    logger.info(f"  {len(existing)} datasets ready in streaming_datasets/:")
    for f in existing:
        logger.info(f"    - {f}")
    return existing


def run_once_now(scheduler):
    """Trigger an immediate single-cycle stream."""
    logger.info("\n[IMMEDIATE] Triggering initial stream cycle ...")
    try:
        result = scheduler.stream_dataset()
        print(f"\n{'─'*60}")
        print(f"  Immediate cycle result:")
        print(f"  Dataset   : {result.get('dataset')}")
        print(f"  Records   : {result.get('records_ingested')}")
        print(f"  Quality   : {result.get('quality_score')}% ({result.get('quality_status')})")
        print(f"  Facts     : {result.get('facts_stored')}  | Skills: {result.get('skills_extracted')}")
        print(f"  Duration  : {result.get('duration_seconds')}s")
        print(f"  Next      : {result.get('next_dataset')}")
        print(f"{'─'*60}\n")
    except Exception as e:
        logger.error(f"Immediate stream cycle failed: {e}", exc_info=True)


def print_status(scheduler, cycle_count: int):
    """Pretty-print scheduler status every cycle."""
    status = scheduler.get_status()
    print(f"\n{'─'*60}")
    print(f"  [Cycle #{cycle_count}]  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Scheduler : {status.get('status')}")
    print(f"  Queue     : {status.get('total_datasets_in_queue')} datasets")
    print(f"  Next      : {status.get('current_dataset')}")
    print(f"  Countdown : {status.get('seconds_until_next_run')}s until next run")
    print(f"  Total ran : {status.get('total_streamed_count')} cycles")
    if status.get('latest_result'):
        r = status['latest_result']
        print(f"  Last run  : {r.get('dataset')} | {r.get('records_ingested')} records | "
              f"Q={r.get('quality_score')}% | {r.get('duration_seconds')}s")
    print(f"{'─'*60}")


def main():
    parser = argparse.ArgumentParser(description="JobPulse 1-Minute Streaming Pipeline")
    parser.add_argument("--now",        action="store_true", help="Run one streaming cycle immediately on start")
    parser.add_argument("--regenerate", action="store_true", help="Regenerate all datasets before starting")
    parser.add_argument("--interval",   type=int, default=60, help="Streaming interval in seconds (default: 60)")
    parser.add_argument("--list",       action="store_true", help="List datasets and exit")
    args = parser.parse_args()

    banner()

    # ─── Step 1: Generate datasets ───────────────────────────────────────────
    generate_datasets(regenerate=args.regenerate)

    if args.list:
        sys.exit(0)

    # ─── Step 2: Initialize scheduler ────────────────────────────────────────
    from kafka_stream_scheduler import get_kafka_scheduler
    scheduler = get_kafka_scheduler(interval_seconds=args.interval)

    logger.info(f"Scheduler initialized with {len(scheduler.datasets)} datasets in queue.")
    logger.info(f"Streaming interval: {args.interval}s | Starting at dataset index: {scheduler.current_index}")
    logger.info(f"Dataset rotation: {scheduler.datasets}")

    # ─── Step 3: Optional immediate run ──────────────────────────────────────
    if args.now:
        run_once_now(scheduler)

    # ─── Step 4: Start background scheduler ──────────────────────────────────
    scheduler.start()
    logger.info(f"\nScheduler STARTED. Streaming one dataset every {args.interval}s.")
    logger.info("Press Ctrl+C to stop.\n")

    # ─── Step 5: Monitor loop ─────────────────────────────────────────────────
    cycle_count = 0
    last_streamed_count = scheduler.total_streamed

    def handle_shutdown(signum, frame):
        print(f"\n\n{SEPARATOR}")
        logger.info("Shutdown signal received. Stopping scheduler ...")
        scheduler.stop()
        status = scheduler.get_status()
        print(f"\n  Final Stats:")
        print(f"    Total cycles completed : {status.get('total_streamed_count')}")
        print(f"    Datasets in queue      : {status.get('total_datasets_in_queue')}")
        if status.get('latest_result'):
            r = status['latest_result']
            print(f"    Last dataset streamed  : {r.get('dataset')}")
            print(f"    Last quality score     : {r.get('quality_score')}%")
        print(f"{SEPARATOR}\n")
        sys.exit(0)

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        while True:
            time.sleep(10)  # Poll status every 10 seconds
            cycle_count += 1

            current_streamed = scheduler.total_streamed
            if current_streamed != last_streamed_count:
                # A new cycle just completed - print result
                last_streamed_count = current_streamed
                if scheduler.latest_result:
                    r = scheduler.latest_result
                    print(f"\n{SEPARATOR}")
                    print(f"  STREAM CYCLE COMPLETE  [{datetime.now().strftime('%H:%M:%S')}]")
                    print(f"  Dataset   : {r.get('dataset')}")
                    print(f"  Kafka Mode: {r.get('kafka_mode')}")
                    print(f"  Records   : {r.get('records_ingested')}")
                    print(f"  Quality   : {r.get('quality_score')}% ({r.get('quality_status')})")
                    print(f"  Facts     : {r.get('facts_stored')} | Skills: {r.get('skills_extracted')}")
                    print(f"  DuckDB    : {r.get('duckdb_olap_latency_ms')}ms OLAP latency")
                    print(f"  Duration  : {r.get('duration_seconds')}s")
                    print(f"  Up next   : {r.get('next_dataset')}")
                    print(f"  Total runs: {current_streamed}")
                    print(f"{SEPARATOR}\n")
            else:
                # Print countdown every 30 seconds (every 3rd 10-second poll)
                if cycle_count % 3 == 0:
                    status = scheduler.get_status()
                    countdown = status.get("seconds_until_next_run", 0)
                    current_ds = status.get("current_dataset", "?")
                    logger.info(
                        f"[{status.get('status')}] Next: {current_ds} "
                        f"| Countdown: {countdown}s "
                        f"| Completed: {status.get('total_streamed_count')} cycles"
                    )

    except (KeyboardInterrupt, SystemExit):
        handle_shutdown(None, None)


if __name__ == "__main__":
    main()

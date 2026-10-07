"""
JobPulse - Local Run Script (No Docker Required)
Runs the complete ETL pipeline with automatic retries, background scheduling,
SQLite database, and starts the Flask analytics dashboard.

Usage:
  python run_local.py                    # Runs initial ETL, starts scheduler (every 5m) & dashboard
  python run_local.py --interval 10      # Runs ETL every 10 minutes
  python run_local.py --retries 5        # Configures 5 retries per task on failure
  python run_local.py --run-once         # Executes pipeline once with retries and exits
  python run_local.py --no-schedule      # Starts dashboard without recurring schedule
"""
import os
import sys
import subprocess
import argparse
import signal

# Fix Windows console encoding
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Set environment for SQLite mode
os.environ['USE_SQLITE'] = '1'

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'scripts')
DASHBOARD_DIR = os.path.join(PROJECT_ROOT, 'dashboard')

if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)
if DASHBOARD_DIR not in sys.path:
    sys.path.insert(0, DASHBOARD_DIR)


def install_deps():
    """Install required packages if missing."""
    required = ['flask', 'flask_cors', 'pandas', 'numpy', 'sqlalchemy']
    missing = []
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    
    if missing:
        print(f"[*] Installing missing packages: {', '.join(missing)}")
        pip_names = [p.replace('_', '-') for p in missing]
        subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + pip_names,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("[OK] Packages installed.")


def parse_args():
    parser = argparse.ArgumentParser(description="JobPulse Local ETL Pipeline & Dashboard Runner")
    parser.add_argument(
        '-i', '--interval',
        type=int,
        default=int(os.environ.get('PIPELINE_INTERVAL_MINUTES', 5)),
        help="Recurring schedule interval in minutes (default: 5)"
    )
    parser.add_argument(
        '-r', '--retries',
        type=int,
        default=int(os.environ.get('PIPELINE_MAX_RETRIES', 3)),
        help="Max retry attempts per task on failure (default: 3)"
    )
    parser.add_argument(
        '-d', '--retry-delay',
        type=int,
        default=int(os.environ.get('PIPELINE_RETRY_DELAY', 2)),
        help="Initial retry delay in seconds with exponential backoff (default: 2)"
    )
    parser.add_argument(
        '--run-once',
        action='store_true',
        help="Execute pipeline once with retries and exit (no dashboard or scheduler)"
    )
    parser.add_argument(
        '--no-schedule',
        action='store_true',
        help="Disable recurring background scheduling (run once at start, then keep dashboard running)"
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help="Dashboard port (default: 5000)"
    )
    return parser.parse_args()


def print_banner(args):
    print("\n" + "=" * 65)
    print("  🚀 JobPulse - Automated Job Market Data Pipeline (Local Mode)")
    print("=" * 65)
    print(f"  • Database Mode    : SQLite ({os.path.join('data', 'jobpulse.db')})")
    print(f"  • Task Retries     : {args.retries} attempts (exponential backoff)")
    print(f"  • Retry Base Delay : {args.retry_delay}s")
    if args.run_once:
        print("  • Mode             : Single Execution (--run-once)")
    elif args.no_schedule:
        print("  • Scheduler        : Disabled (Manual / API trigger only)")
    else:
        print(f"  • Auto Scheduler   : Active (Runs every {args.interval} minutes)")
    if not args.run_once:
        print(f"  • Dashboard URL    : http://localhost:{args.port}")
    print("=" * 65 + "\n")


def main():
    args = parse_args()
    install_deps()
    print_banner(args)
    
    from pipeline_runner import run_pipeline, get_global_scheduler
    
    # 1. Execute initial pipeline run with retries
    print("▶️ Executing initial pipeline run...")
    result = run_pipeline(
        triggered_by="startup",
        max_retries=args.retries,
        retry_delay=args.retry_delay
    )
    
    if args.run_once:
        print("\n" + "=" * 65)
        print(f"  Run Completed: Status = {result.get('status')}")
        print("=" * 65)
        sys.exit(0 if result.get('status') == 'SUCCESS' else 1)
        
    # 2. Start Background Scheduler if enabled
    scheduler = get_global_scheduler(
        interval_minutes=args.interval,
        max_retries=args.retries,
        retry_delay=args.retry_delay
    )
    
    if not args.no_schedule:
        scheduler.start()
        print(f"⏰ Background Scheduler running: Next run in ~{args.interval} minutes.")
    
    # 3. Handle graceful termination
    def handle_exit(signum, frame):
        print("\n[*] Shutting down scheduler and dashboard...")
        if scheduler.running:
            scheduler.stop()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    
    # 4. Start Flask Dashboard
    print(f"\n🌐 Starting Dashboard at http://localhost:{args.port}")
    print("   Press Ctrl+C to stop.\n")
    
    os.chdir(DASHBOARD_DIR)
    from app import app
    app.run(host='0.0.0.0', port=args.port, debug=False)


if __name__ == '__main__':
    main()

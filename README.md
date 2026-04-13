EpiCollect5 -> SQL Server | Production Sync
Project: daily-sales-tigers-brewery

Usage:
    python epicollect.py               # sync last N days (from .env), silent
    python epicollect.py --days 30     # override: last 30 days
    python epicollect.py --full        # full sync, no date filter
    python epicollect.py --verbose     # show progress in terminal

Log file records:
    - Each run: date, inserted count, skipped count, duration
    - Errors only (auth failures, insert errors, connection issues)

Requirements:
    pip install requests pyodbc python-dotenv tenacity

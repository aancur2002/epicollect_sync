"""
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
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Optional

import pyodbc
import requests
from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ──────────────────────────────────────────────────────────────────
# LOAD .env
# ──────────────────────────────────────────────────────────────────
load_dotenv()


def _env(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        raise EnvironmentError(f"Missing required config: '{key}' not set in .env")
    return val


def _env_opt(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


# ──────────────────────────────────────────────────────────────────
# LOGGING
#
# Log FILE records only:
#   [RUN]   one line per run  — date, inserted, skipped, duration
#   [ERROR] any failure       — auth, connection, insert errors
#
# Console output only when --verbose flag is used.
# ASCII-safe handler avoids cp1252 errors on Windows terminals.
# ──────────────────────────────────────────────────────────────────
class _SafeStreamHandler(logging.StreamHandler):
    """Strips non-ASCII symbols before writing to Windows console."""
    _TABLE = str.maketrans({
        "\u2550": "=", "\u2500": "-", "\u2192": "->",
        "\u2713": "[OK]", "\u21b7": "[SKIP]", "\u23f1": "[TIME]",
    })
    def emit(self, record: logging.LogRecord):
        record.msg = str(record.msg).translate(self._TABLE)
        super().emit(record)


class _RunAndErrorFilter(logging.Filter):
    """Allow only [RUN] tagged WARNING messages and ERROR/CRITICAL to file."""
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return True
        if record.levelno == logging.WARNING and str(record.msg).startswith("[RUN]"):
            return True
        return False


def _setup_logging(log_file: str, verbose: bool) -> logging.Logger:
    logger = logging.getLogger("ec5sync")
    logger.setLevel(logging.DEBUG)

    # File handler — only [RUN] summaries and errors reach the file
    fh = RotatingFileHandler(
        log_file, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.WARNING)
    fh.addFilter(_RunAndErrorFilter())
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s"))
    logger.addHandler(fh)

    # Console — only when --verbose
    if verbose:
        ch = _SafeStreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s"))
        logger.addHandler(ch)

    return logger


log = logging.getLogger("ec5sync")  # replaced in main() after args parsed


def _log_run(msg: str):
    """Write one run-summary line to the log file."""
    log.warning(f"[RUN] {msg}")


# ──────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────
def load_config() -> dict:
    trusted = _env_opt("SQL_TRUSTED", "yes").lower() == "yes"
    if trusted:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={_env('SQL_SERVER')};"
            f"DATABASE={_env('SQL_DATABASE')};"
            f"Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={_env('SQL_SERVER')};"
            f"DATABASE={_env('SQL_DATABASE')};"
            f"UID={_env('SQL_UID')};"
            f"PWD={_env('SQL_PWD')};"
        )
    return {
        "client_id":     _env("EC5_CLIENT_ID"),
        "client_secret": _env("EC5_CLIENT_SECRET"),
        "project_slug":  _env("EC5_PROJECT_SLUG"),
        "sql_conn_str":  conn_str,
        "table":         _env_opt("SQL_TABLE",      "DBO.EPICOLLECT"),
        "unique_col":    _env_opt("SQL_UNIQUE_COL", "EC5"),
        "default_days":  int(_env_opt("DEFAULT_DAYS", "7")),
        "per_page":      int(_env_opt("PER_PAGE",     "1000")),
        "max_workers":   int(_env_opt("MAX_WORKERS",  "4")),
        "page_delay":    float(_env_opt("PAGE_DELAY", "0.1")),
    }


# ──────────────────────────────────────────────────────────────────
# EXACT COLUMN MAP  —  verified against ec5_diagnose.py output
#
# Format:
#   "exact_ec5_api_field": ("scalar", "SQL_COLUMN")
#   "exact_ec5_api_field": ("gps",    ("LAT_COL", "LON_COL", "ACC_COL"))
# ──────────────────────────────────────────────────────────────────
COLUMN_MAP: dict[str, tuple] = {
    # System fields
    "ec5_uuid":              ("scalar", "EC5"),
    "created_at":            ("scalar", "CREATED"),
    "uploaded_at":           ("scalar", "UPLOADED"),
    "created_by":            ("scalar", "CREATEDBY"),
    "title":                 ("scalar", "TITLE"),
    # Survey fields
    "1_Zone_Name":           ("scalar", "ZONE"),
    "2_Bagmati":             ("scalar", "BAGMATI"),
    "3_Bagmati_Valley":      ("scalar", "VALLEY"),
    "4_Center_Dealers":      ("scalar", "CENTER"),
    "5_East_Dealers":        ("scalar", "EAST"),
    "6_Mid_East_Dealers":    ("scalar", "MIDEAST"),
    "7_Narayani_Dealers":    ("scalar", "NARAYANI"),
    "8_Lumbini_Dealers":     ("scalar", "LUMBINI"),
    "9_West_Dealers":        ("scalar", "WEST"),
    "10_Far_West_Dealer":    ("scalar", "FARWEST"),
    "12_Type_of_Outlet":     ("scalar", "OUTLETTYPE"),
    "13_Outlet_Name":        ("scalar", "OUTLETNAME"),
    "14_Outlet_Address":     ("scalar", "OUTLETADD"),
    "15_Outlet_MobilePhon":  ("scalar", "OUTLETPH"),
    "16_Is_there_Any_Asse":  ("scalar", "FRIDGE"),
    "17_Is_it_in_Working_":  ("scalar", "WORKING"),
    "19_Is_there_any_our_":  ("scalar", "PRODUCTS"),
    "20_Remarks":            ("scalar", "REMARKS"),
    "22_TBL_Bottle":         ("scalar", "TBLBOT"),
    "23_BB_Date_of_TBL_Bo":  ("scalar", "BBTBL"),
    "24_TBL_Can":            ("scalar", "TBLCAN"),
    "25_BB_Date_of_TBL_Ca":  ("scalar", "BBTBLC"),
    "26_TBL_330":            ("scalar", "TBL_330"),
    "27_BB_DATE_OF_TBL_33":  ("scalar", "BBTBL330"),
    "28_TBS_Bottle":         ("scalar", "TBSBOT"),
    "29_BB_Date_of_TBS_Bo":  ("scalar", "BBTBS"),
    "30_TBS_Can":            ("scalar", "TBSCAN"),
    "31_BB_Date_of_TBS_Ca":  ("scalar", "BBTBSC"),
    "32_TBS_330":            ("scalar", "TBSC_330"),
    "33_BB_Date_of_TBS_33":  ("scalar", "BBTBS330"),
    "34_NTS_Bottle":         ("scalar", "NTSBOT"),
    "35_BB_Date_of_NTS_Bo":  ("scalar", "BBNTSB"),
    "36_NTS_Can":            ("scalar", "NTSCAN"),
    "37_BB_Date_of_NTS_Ca":  ("scalar", "BBNTSC"),
    "38_NTS_330":            ("scalar", "NTS_330"),
    "39_BB_Date_of_NTS_33":  ("scalar", "BBNTS330"),
    "40_LIS_Bottle":         ("scalar", "LISBOT"),
    "41_BB_Date_of_LIS_Bo":  ("scalar", "BBLISB"),
    "42_LIS_Can":            ("scalar", "LISCAN"),
    "43_BB_Date_of_LIS_Ca":  ("scalar", "BBLISCAN"),
    "44_LIS_330":            ("scalar", "LIS_330"),
    "45_BB_Date_of_LIS_33":  ("scalar", "BBLIS330"),
    "46_LIS_5000":           ("scalar", "LIS_5000"),
    "47_BB_Date_of_LIS_50":  ("scalar", "BBLIS5000"),
    "48_RSS_Bottle":         ("scalar", "RSSBOT"),
    "49_BB_Date_of_RSS_Bo":  ("scalar", "BBRSSB"),
    "50_Rockstar_330":       ("scalar", "RSSB330"),
    "51_BB_Date_of_Rockst":  ("scalar", "BBRSS330"),
    "52_Annapurna_Strong_":  ("scalar", "APS_330"),
    "53_BB_Date_of_Annapu":  ("scalar", "BBAP330"),
    "54_Marsi_Premier_Str":  ("scalar", "MARSIBOT"),
    "55_BB_Date_of_Marsi_":  ("scalar", "BBMARSI"),
    "56_Marsi_Premier_Str":  ("scalar", "MARSIJHYAP"),
    "57_BB_Date_of_Marsi_":  ("scalar", "BBMJHYAP"),
    "58_New_Product_1":      ("scalar", "PRO1"),
    "59_BB_Date_of_New_1":   ("scalar", "BBPRO1"),
    "60_New_Product_2":      ("scalar", "PRO2"),
    "61_BB_Date_of_New_2":   ("scalar", "BBPRO2"),
    "62_Sales_Order__Visi":  ("scalar", "SALESTYPE"),
    "64_TBL_Bottlecase":     ("scalar", "TBLB"),
    "65_TBL_Cancase":        ("scalar", "TBLC"),
    "66_TBL_330case":        ("scalar", "TBL330"),
    "67_TBS_Bottlecase":     ("scalar", "TBSB"),
    "68_TBS_Cancase":        ("scalar", "TBSC"),
    "69_TBS_330case":        ("scalar", "TBS330"),
    "70_NTS_Bottlecase":     ("scalar", "NTSB"),
    "71_NTS_Cancase":        ("scalar", "NTSC"),
    "72_NTS_330_case":       ("scalar", "NTS330"),
    "73_LIS_Bottlecase":     ("scalar", "LISB"),
    "74_LIS_Cancase":        ("scalar", "LISC"),
    "75_LIS_330case":        ("scalar", "LIS330"),
    "76_LIS_5000case":       ("scalar", "LIS5000"),
    "77_RSS_Bottle_650_ca":  ("scalar", "RSSB"),
    "78_Rockstar_330case":   ("scalar", "RSS330"),
    "79_Annapurna_Strong_":  ("scalar", "ABS330"),
    "80_Marsi_Premium_Str":  ("scalar", "MARSI"),
    "81_Marsi_Premier_Str":  ("scalar", "MARSI330"),
    "82_New_1":              ("scalar", "New_1"),
    "83_New_2":              ("scalar", "New_2"),
    "85_Competitor_Feedba":  ("scalar", "COMP_FEED"),
    "86_Your_Message":       ("scalar", "YOUR_MESSAGE"),
    # GPS — expanded into LATITUE, LONGITUDE, ACC_LOC
    "87_Location":           ("gps", ("LATITUE", "LONGITUDE", "ACC_LOC")),
}

SQL_COLUMNS = [
    "EC5", "CREATED", "UPLOADED", "CREATEDBY", "TITLE",
    "ZONE", "BAGMATI", "VALLEY", "CENTER", "EAST", "MIDEAST",
    "NARAYANI", "LUMBINI", "WEST", "FARWEST",
    "OUTLETTYPE", "OUTLETNAME", "OUTLETADD", "OUTLETPH",
    "FRIDGE", "WORKING", "PRODUCTS", "REMARKS",
    "TBLBOT", "BBTBL", "TBLCAN", "BBTBLC", "TBL_330", "BBTBL330",
    "TBSBOT", "BBTBS", "TBSCAN", "BBTBSC", "TBSC_330", "BBTBS330",
    "NTSBOT", "BBNTSB", "NTSCAN", "BBNTSC", "NTS_330", "BBNTS330",
    "LISBOT", "BBLISB", "LISCAN", "BBLISCAN", "LIS_330", "BBLIS330",
    "LIS_5000", "BBLIS5000",
    "RSSBOT", "BBRSSB", "RSSB330", "BBRSS330",
    "APS_330", "BBAP330",
    "MARSIBOT", "BBMARSI", "MARSIJHYAP", "BBMJHYAP",
    "PRO1", "BBPRO1", "PRO2", "BBPRO2",
    "SALESTYPE",
    "TBLB", "TBLC", "TBL330", "TBSB", "TBSC", "TBS330",
    "NTSB", "NTSC", "NTS330", "LISB", "LISC", "LIS330", "LIS5000",
    "RSSB", "RSS330", "ABS330", "MARSI", "MARSI330",
    "New_1", "New_2", "COMP_FEED", "YOUR_MESSAGE",
    "LATITUE", "LONGITUDE", "ACC_LOC",
]


# ──────────────────────────────────────────────────────────────────
# AUTHENTICATION
# ──────────────────────────────────────────────────────────────────
def get_token(cfg: dict) -> str:
    log.info("Authenticating ...")
    resp = requests.post(
        "https://five.epicollect.net/api/oauth/token",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "grant_type":    "client_credentials",
            "client_id":     cfg["client_id"],
            "client_secret": cfg["client_secret"],
        }),
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError("No access_token returned by EpiCollect5.")
    return token


# ──────────────────────────────────────────────────────────────────
# DATA FETCHING
# ──────────────────────────────────────────────────────────────────
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _api_get(url: str, headers: dict, params: dict) -> dict:
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code == 429:
        wait = int(resp.headers.get("Retry-After", 60))
        log.warning(f"Rate limited - sleeping {wait}s ...")
        time.sleep(wait)
        resp.raise_for_status()
    resp.raise_for_status()
    return resp.json()


def _build_params(f_date: str, l_date: str, page: int, per_page: int) -> dict:
    return {
        "sort_by":     "created_at",
        "sort_order":  "DESC",
        "filter_by":   "created_at",
        "filter_from": f_date,
        "filter_to":   l_date,
        "per_page":    per_page,
        "page":        page,
    }


def _fetch_page(url: str, headers: dict, page: int,
                f_date: str, l_date: str,
                per_page: int, delay: float) -> list[dict]:
    time.sleep(delay)
    body = _api_get(url, headers, _build_params(f_date, l_date, page, per_page))
    return body.get("data", {}).get("entries", [])


def fetch_all_entries(token: str, cfg: dict, f_date: str, l_date: str) -> list[dict]:
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}",
    }
    url = f"https://five.epicollect.net/api/export/entries/{cfg['project_slug']}"

    log.info(f"Fetching entries {f_date} to {l_date}")
    body      = _api_get(url, headers, _build_params(f_date, l_date, 1, cfg["per_page"]))
    meta      = body.get("meta", {})
    last_page = meta.get("last_page", 1)
    entries   = body.get("data", {}).get("entries", [])

    if last_page > 1:
        log.info(f"Fetching pages 2-{last_page} in parallel ...")
        with ThreadPoolExecutor(max_workers=cfg["max_workers"]) as pool:
            futures = {
                pool.submit(
                    _fetch_page, url, headers, p,
                    f_date, l_date, cfg["per_page"], cfg["page_delay"]
                ): p
                for p in range(2, last_page + 1)
            }
            for future in as_completed(futures):
                p = futures[future]
                try:
                    entries.extend(future.result())
                except Exception as e:
                    log.error(f"Page {p} fetch failed: {e}")

    return entries


# ──────────────────────────────────────────────────────────────────
# TRANSFORM
# ──────────────────────────────────────────────────────────────────
def transform_entry(entry: dict) -> Optional[dict]:
    row: dict = {}
    for ec5_field, (kind, target) in COLUMN_MAP.items():
        value = entry.get(ec5_field)
        if kind == "gps":
            lat_col, lon_col, acc_col = target
            if isinstance(value, dict):
                row[lat_col] = value.get("latitude")
                row[lon_col] = value.get("longitude")
                row[acc_col] = value.get("accuracy")
            else:
                row[lat_col] = row[lon_col] = row[acc_col] = None
        else:
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            row[target] = None if value == "" else value

    if not row.get("EC5"):
        return None
    return {col: row.get(col) for col in SQL_COLUMNS}


# ──────────────────────────────────────────────────────────────────
# SQL SERVER
# ──────────────────────────────────────────────────────────────────
def _table_short(table: str) -> str:
    return table.split(".")[-1].strip("[]").strip()


def ensure_columns_exist(conn, table: str, unique_col: str):
    cursor    = conn.cursor()
    tbl_short = _table_short(table)

    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", tbl_short
    )
    if not cursor.fetchone():
        col_defs = ",\n    ".join(f"[{c}] NVARCHAR(MAX)" for c in SQL_COLUMNS)
        pk = (
            f",\n    CONSTRAINT [PK_{tbl_short}] PRIMARY KEY ([{unique_col}])"
            if unique_col in SQL_COLUMNS else ""
        )
        cursor.execute(f"CREATE TABLE {table} (\n    {col_defs}{pk}\n)")
        conn.commit()
        cursor.close()
        return

    cursor.execute(
        "SELECT UPPER(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?",
        tbl_short,
    )
    existing = {r[0] for r in cursor.fetchall()}
    for col in [c for c in SQL_COLUMNS if c.upper() not in existing]:
        cursor.execute(f"ALTER TABLE {table} ADD [{col}] NVARCHAR(MAX)")
        log.warning(f"[RUN] New column added to {table}: {col}")
    conn.commit()
    cursor.close()


def bulk_insert(conn, cfg: dict, rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    table      = cfg["table"]
    unique_col = cfg["unique_col"]

    # Step 1: load existing UUIDs (one query)
    cursor = conn.cursor()
    cursor.execute("SELECT [" + unique_col + "] FROM " + table)
    existing = {r[0] for r in cursor.fetchall() if r[0]}
    cursor.close()
    new_rows = [row for row in rows if row.get("EC5") not in existing]
    skipped  = len(rows) - len(new_rows)

    if not new_rows:
        return 0, skipped

    col_list = ", ".join("[" + c + "]" for c in SQL_COLUMNS)

    def _safe(v):
        """Embed value directly in SQL string — avoids ODBC parameter round-trips."""
        if v is None:
            return "NULL"
        return "N'" + str(v).replace("'", "''") + "'"

    # Step 2: INSERT via SELECT ... UNION ALL — one SQL call per chunk
    # All values are embedded as literals so there are zero ODBC parameters.
    # This means one network round-trip per chunk regardless of row count.
    CHUNK    = 500
    inserted = 0

    for i in range(0, len(new_rows), CHUNK):
        chunk    = new_rows[i : i + CHUNK]
        row_strs = []
        for row in chunk:
            vals = ", ".join(_safe(row.get(c)) for c in SQL_COLUMNS)
            row_strs.append("SELECT " + vals)
        union_sql = " UNION ALL ".join(row_strs)
        sql = "INSERT INTO " + table + " (" + col_list + ") " + union_sql

        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            inserted += len(chunk)
            pass  # chunk inserted successfully
        except Exception as e:
            log.error("Chunk %d failed (%s: %s) - row-by-row fallback" % (i//CHUNK+1, type(e).__name__, e))
            row_ph  = ", ".join(["?"] * len(SQL_COLUMNS))
            row_sql = "INSERT INTO " + table + " (" + col_list + ") VALUES (" + row_ph + ")"
            for row in chunk:
                try:
                    cursor.execute(row_sql, [row.get(c) for c in SQL_COLUMNS])
                    inserted += 1
                except Exception as re:
                    log.error("Row skipped EC5=%s: %s" % (row.get("EC5"), re))
        finally:
            cursor.close()

    conn.autocommit = False
    return inserted, skipped


def parse_args(default_days: int) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="EpiCollect5 -> SQL Server incremental sync")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--days", type=int, default=default_days,
                   help=f"Fetch entries from the last N days (default: {default_days})")
    g.add_argument("--full", action="store_true",
                   help="Full sync - no date filter")
    p.add_argument("--verbose", action="store_true",
                   help="Print progress to terminal (default: silent)")
    return p.parse_args()


def main():
    try:
        cfg = load_config()
    except EnvironmentError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)

    args = parse_args(cfg["default_days"])

    global log
    log = _setup_logging(_env_opt("LOG_FILE", "epicollect5_sync.log"), args.verbose)

    start  = datetime.now()
    l_date = datetime.now()
    f_date = datetime(2000, 1, 1) if args.full else l_date - timedelta(days=args.days)

    log.info(f"Sync started | project={cfg['project_slug']} | table={cfg['table']}")
    log.info(f"Mode: {'FULL' if args.full else f'last {args.days} day(s)'}")

    # Step 1: Authenticate
    try:
        token = get_token(cfg)
    except Exception as e:
        log.error(f"Authentication failed: {e}")
        _log_run(f"FAILED | auth error | {e}")
        sys.exit(1)

    # Step 2: Fetch
    try:
        raw_entries = fetch_all_entries(token, cfg, f_date.isoformat(), l_date.isoformat())
    except Exception as e:
        log.error(f"Fetch failed: {e}")
        _log_run(f"FAILED | fetch error | {e}")
        sys.exit(1)

    if not raw_entries:
        _log_run(f"OK | 0 inserted | 0 skipped | 0.0s | no entries on server")
        return

    # Step 3: Transform
    rows = [r for e in raw_entries if (r := transform_entry(e)) is not None]

    # Step 4: Connect to SQL Server
    try:
        conn = pyodbc.connect(cfg["sql_conn_str"], timeout=0)
        conn.autocommit = False
        conn.timeout = 0  # disable statement timeout — long inserts won't get cut off
    except Exception as e:
        log.error(f"SQL Server connection failed: {e}")
        _log_run(f"FAILED | db connection error | {e}")
        sys.exit(1)

    try:
        ensure_columns_exist(conn, cfg["table"], cfg["unique_col"])
        inserted, skipped = bulk_insert(conn, cfg, rows)
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        _log_run(f"FAILED | unexpected error | {e}")
        sys.exit(1)
    finally:
        conn.close()

    # Write the single run-summary line to the log file
    elapsed = (datetime.now() - start).total_seconds()
    _log_run(
        f"OK | inserted={inserted:,} | skipped={skipped:,} | "
        f"duration={elapsed:.1f}s | table={cfg['table']}"
    )


if __name__ == "__main__":
    main()

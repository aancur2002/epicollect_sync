# 📊 EpiCollect5 → SQL Server | Production Sync  
**Project Name:** `daily-sales-downloader`

---

## 🚀 Overview
This project synchronizes data from **EpiCollect5** to **SQL Server** for production use.  
It is designed for automated daily sales data extraction, transformation, and storage.

---

## ⚙️ Usage

Run the script with different options based on your needs:

```bash
python epicollect.py               # Sync last N days (defined in .env), silent
python epicollect.py --days 30     # Override: sync last 30 days
python epicollect.py --full        # Full sync (no date filter)
python epicollect.py --verbose     # Show progress in terminal

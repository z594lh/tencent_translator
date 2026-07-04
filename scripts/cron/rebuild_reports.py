#!/usr/bin/env python3
"""
消化日报重建任务 (report_rebuild_tasks)

用法:
    python scripts/cron/rebuild_reports.py

定时: 建议每小时执行
      crontab: 30 * * * * cd /path && python scripts/cron/rebuild_reports.py >> log/rebuild_reports.log 2>&1
"""
import os, sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from datetime import datetime
from scripts.cron import _now_str
from services.mysql_service import get_db_connection
from services.report_generator import generate_business_daily, generate_advertising_daily


def run():
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("SELECT id, report_date, task_type FROM report_rebuild_tasks WHERE status = 0 ORDER BY id")
            tasks = c.fetchall()

        if not tasks:
            print(f"[{_now_str()}] [Rebuild] no pending tasks")
            return

        print(f"[{_now_str()}] [Rebuild] {len(tasks)} pending tasks")
        for task in tasks:
            report_date = str(task['report_date'])
            task_type = task['task_type']
            print(f"  [{task_type}] {report_date} ...", end=" ")
            try:
                if task_type == 'business':
                    generate_business_daily(report_date)
                elif task_type == 'advertising':
                    generate_advertising_daily(report_date)
                with conn.cursor() as c:
                    c.execute("UPDATE report_rebuild_tasks SET status=1 WHERE id=%s", (task['id'],))
                conn.commit()
                print("OK")
            except Exception as e:
                print(f"FAIL: {e}")
                try:
                    with conn.cursor() as c:
                        c.execute("UPDATE report_rebuild_tasks SET status=2 WHERE id=%s", (task['id'],))
                    conn.commit()
                except:
                    pass
    finally:
        conn.close()


if __name__ == '__main__':
    run()

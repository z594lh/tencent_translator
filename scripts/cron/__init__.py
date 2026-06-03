"""计划任务公共模块：路径配置 + 工具函数"""
import os
import sys

# 把项目根目录加入 PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 加载 .env 配置（crontab 不会自动加载环境变量）
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

import requests
from datetime import datetime
from services.mysql_service import get_db_connection


def _now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def fetch_and_save_exchange_rate(from_currency='CNY', to_currency='USD'):
    try:
        url = f"https://open.er-api.com/v6/latest/{from_currency}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get('result') != 'success':
            print(f"[ExchangeRate] API 返回非 success: {data}")
            return None

        rate = data.get('rates', {}).get(to_currency)
        if rate is None:
            print(f"[ExchangeRate] 未找到 {from_currency}->{to_currency} 汇率")
            return None

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO exchange_rates (from_currency, to_currency, rate, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        rate = VALUES(rate),
                        updated_at = VALUES(updated_at)
                """, (from_currency, to_currency, rate))
                conn.commit()
        finally:
            conn.close()

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{now}] [ExchangeRate] 汇率更新成功: {from_currency}->{to_currency} = {rate}")
        return {'rate': rate, 'updated_at': now}
    except Exception as e:
        print(f"[ExchangeRate] 获取汇率异常: {e}")
        return None

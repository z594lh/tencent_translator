#!/usr/bin/env python3
"""
自动完结超期订单（每天凌晨1:30）
进货单/货代运单创建10天后仍为初始状态，自动变更为已完成并入账
"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from datetime import datetime
from scripts.cron import _now_str
from services.mysql_service import get_db_connection


def run():
    from blueprints.supplier import PURCHASE_ORDER_STATUS_INITIAL, PURCHASE_ORDER_STATUS_COMPLETED
    from blueprints.logistics import WAYBILL_STATUS_INITIAL, WAYBILL_STATUS_COMPLETED
    from blueprints.expenses import create_expense_for_source

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # ---------- 进货单 ----------
            cursor.execute("""
                SELECT id, order_no, total_amount
                FROM purchase_orders
                WHERE status = %s AND created_at <= DATE_SUB(NOW(), INTERVAL 10 DAY)
            """, (PURCHASE_ORDER_STATUS_INITIAL,))
            stale_orders = cursor.fetchall()

            if stale_orders:
                print(f"[{_now_str()}] [Cron] 发现 {len(stale_orders)} 条超期进货单，开始自动完结...")
                for order in stale_orders:
                    try:
                        cursor.execute(
                            "UPDATE purchase_orders SET status = %s WHERE id = %s",
                            (PURCHASE_ORDER_STATUS_COMPLETED, order['id'])
                        )
                        cursor.execute(
                            "SELECT id FROM expenses WHERE source_type = %s AND source_no = %s LIMIT 1",
                            ('purchase_order', order['order_no'])
                        )
                        if not cursor.fetchone() and order['order_no']:
                            create_expense_for_source(
                                conn, '采购/货值',
                                float(order['total_amount'] or 0),
                                datetime.now().strftime('%Y-%m-%d'),
                                f"进货单 {order['order_no']}（自动完结）",
                                'purchase_order', order['order_no'], 'company'
                            )
                        print(f"[{_now_str()}] [Cron]   进货单 {order['order_no']} 已自动完结并入账")
                    except Exception as e:
                        print(f"[{_now_str()}] [Cron]   进货单 {order.get('order_no')} 自动完结异常: {e}")
            else:
                print(f"[{_now_str()}] [Cron] 无超期进货单")

            # ---------- 货代运单 ----------
            cursor.execute("""
                SELECT id, waybill_no, total_cost_cny
                FROM logistics_waybills
                WHERE status = %s AND created_at <= DATE_SUB(NOW(), INTERVAL 10 DAY)
            """, (WAYBILL_STATUS_INITIAL,))
            stale_waybills = cursor.fetchall()

            if stale_waybills:
                print(f"[{_now_str()}] [Cron] 发现 {len(stale_waybills)} 条超期货代运单，开始自动完结...")
                for wb in stale_waybills:
                    try:
                        cursor.execute(
                            "UPDATE logistics_waybills SET status = %s WHERE id = %s",
                            (WAYBILL_STATUS_COMPLETED, wb['id'])
                        )
                        if wb['waybill_no']:
                            cursor.execute(
                                "SELECT id FROM expenses WHERE source_type = %s AND source_no = %s LIMIT 1",
                                ('logistics_waybill', wb['waybill_no'])
                            )
                            if not cursor.fetchone():
                                create_expense_for_source(
                                    conn, '物流/头程',
                                    float(wb['total_cost_cny'] or 0),
                                    datetime.now().strftime('%Y-%m-%d'),
                                    f"运单 {wb['waybill_no']}（自动完结）",
                                    'logistics_waybill', wb['waybill_no'], 'company'
                                )
                        print(f"[{_now_str()}] [Cron]   运单 {wb['waybill_no']} 已自动完结并入账")
                    except Exception as e:
                        print(f"[{_now_str()}] [Cron]   运单 {wb.get('waybill_no')} 自动完结异常: {e}")
            else:
                print(f"[{_now_str()}] [Cron] 无超期货代运单")

            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[{_now_str()}] [Cron] 自动完结超期订单异常: {e}")
    finally:
        conn.close()


if __name__ == '__main__':
    run()

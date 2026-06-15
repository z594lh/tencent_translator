#!/usr/bin/env python3
"""
Amazon Product Fees 定时同步（每天执行一次，凌晨4点）

传固定 $10 参考价调 API，拿到 ReferralFee 金额反除得佣金比例，
连同 FBA 配送费写入 amazon_product_fees 缓存表。佣金比例与售价无关，通用。
"""
import os
import sys
import time
from decimal import Decimal

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from scripts.cron import _now_str
from services.shop_service import get_all_active_shops, get_sp_api_client
from services.mysql_service import get_db_connection

# API 频率控制：每次调用间隔（秒）
_REF_PRICE = 10.0
_MAX_PER_SHOP = -1  # -1 表示不限制，>0 则限制每个店铺最大处理数量


def _get_listings_to_sync(cursor, shop_id: int) -> list:
    """获取店铺下7天内更新的有效子体/独立listing（排除父体）"""
    cursor.execute(
        """SELECT l.sku, l.asin
           FROM amazon_listings l
           WHERE l.shop_id = %s AND l.is_deleted = 0
             AND (l.parentage_level = 'child' OR l.parentage_level IS NULL)
             AND l.asin IS NOT NULL AND l.asin != ''
             AND l.created_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)""",
        (shop_id,),
    )
    return cursor.fetchall()


def run():
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过 Product Fees 同步")
        return

    conn = get_db_connection()
    try:
        for shop in shops:
            shop_id = shop["id"]
            shop_name = shop.get("shop_name", f"shop_{shop_id}")
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始同步 Product Fees...")

            try:
                with conn.cursor() as cursor:
                    listings = _get_listings_to_sync(cursor, shop_id)
            except Exception as e:
                print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 获取 listing 列表失败: {e}")
                continue

            if not listings:
                print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 无可同步的 listing")
                continue

            total = len(listings)
            success = 0
            failed = 0
            skipped = 0

            if _MAX_PER_SHOP > 0:
                listings = listings[:_MAX_PER_SHOP]

            client = get_sp_api_client(shop_id=shop_id)

            for idx, row in enumerate(listings):
                sku = row["sku"]
                asin = row["asin"]

                if idx > 0:
                    time.sleep(1.2)

                try:
                    fee_data = client.get_my_fees_estimate(sku=sku, price_usd=_REF_PRICE)
                    if not fee_data:
                        print(f"[{_now_str()}] [{idx+1}/{total}] {sku}({asin}) API返回空，跳过")
                        skipped += 1
                        continue

                    referral_amount = float(fee_data["referral_fee"])
                    fba_fee = float(fee_data["fba_fee"])
                    commission_rate = referral_amount / _REF_PRICE

                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                """INSERT INTO amazon_product_fees
                                   (shop_id, sku, asin, commission_rate, fba_fee, currency, fetched_at)
                                   VALUES (%s, %s, %s, %s, %s, 'USD', NOW())
                                   ON DUPLICATE KEY UPDATE
                                       asin = VALUES(asin),
                                       commission_rate = VALUES(commission_rate),
                                       fba_fee = VALUES(fba_fee),
                                       fetched_at = NOW()""",
                                (shop_id, sku, asin, commission_rate, fba_fee),
                            )
                        conn.commit()
                        success += 1
                        print(f"[{_now_str()}] [{idx+1}/{total}] {sku}({asin}) OK "
                              f"rate={commission_rate:.4f} fba=${fba_fee:.2f}")
                    except Exception as e:
                        conn.rollback()
                        failed += 1
                        print(f"[{_now_str()}] [{idx+1}/{total}] {sku}({asin}) DB写入失败: {e}")

                except Exception as e:
                    failed += 1
                    print(f"[{_now_str()}] [{idx+1}/{total}] {sku}({asin}) API异常: {e}")

            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 完成: "
                  f"成功{success}, 失败{failed}, 跳过{skipped}, 总计{total}")

    finally:
        conn.close()


if __name__ == "__main__":
    run()

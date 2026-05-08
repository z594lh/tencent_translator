"""
Amazon 业务模块聚合
包含库存、货件、入库计划、发票导出四个子模块
"""
from blueprints.amazon.inventory import amazon_inventory_bp
from blueprints.amazon.shipments import amazon_shipments_bp
from blueprints.amazon.inbound_plans import amazon_inbound_plans_bp
from blueprints.amazon.invoice_export import amazon_invoice_export_bp
from blueprints.amazon.orders import amazon_orders_bp

__all__ = [
    'amazon_inventory_bp',
    'amazon_shipments_bp',
    'amazon_inbound_plans_bp',
    'amazon_invoice_export_bp',
    'amazon_orders_bp',
]

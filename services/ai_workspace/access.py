"""AI工作区显式权限及商品归属；后台同样复核，不依赖菜单可见性。"""
from services.permissions_service import get_user_permission_codes
from services.listing_performance.common import DomainError
from services.ai_operations import plan_store


def authorize(actor,permission='ai_chat:use'):
    codes=set(get_user_permission_codes(actor))
    with plan_store.transaction() as cur:
        cur.execute("SELECT id FROM users WHERE id=%s AND status=1 AND account_type='human'",(actor,))
        if not cur.fetchone() or permission not in codes:raise DomainError('缺少权限：'+permission,403)
    return codes


def predicate(actor,codes,alias='l'):
    """只返回固定SQL片段；用户参数始终绑定，不拼入标识符。"""
    if 'ai_chat:view_all' in codes:return '1=1',[]
    return f'''EXISTS (SELECT 1 FROM listing_responsibility_scopes rs
        JOIN listing_responsibility_history rh ON rh.scope_id=rs.id
        WHERE rs.shop_id={alias}.shop_id AND BINARY rs.marketplace_id=BINARY {alias}.marketplace_id
        AND BINARY rs.seller_sku=BINARY {alias}.sku AND rh.owner_user_id=%s AND rh.voided_at IS NULL
        AND rh.effective_from_utc<=UTC_TIMESTAMP(3)
        AND (rh.effective_to_utc IS NULL OR rh.effective_to_utc>UTC_TIMESTAMP(3)))''',[actor]


def product(actor,target,permission='ai_chat:use'):
    codes=authorize(actor,permission);where,args=predicate(actor,codes)
    with plan_store.transaction() as cur:
        cur.execute('SELECT l.id FROM amazon_listings l WHERE l.id=%s AND l.is_deleted=0 AND '+where,(target['product_id'],*args))
        if not cur.fetchone():raise DomainError('该商品不在当前AI数据授权范围内',403)
    return codes

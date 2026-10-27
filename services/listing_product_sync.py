"""Listing复制到产品的字段解析与保存；只写ERP本地，不向Amazon提交修改。

已有产品不重复生成申报信息；本地AI初稿及人工修订共用保护组，仅固定Listing字段更新。
没有平台型号时保留已有型号；首次新建才按原规则生成型号及申报资料。
"""
import json
from services.mysql_service import get_db_connection
from services.deepseekAI import generate_declaration_info


def _extract_model_from_attrs(attrs):
    """优先读取Listing明确提供的型号属性，不把本地生成型号当作平台来源。"""
    model_list = attrs.get('model', [])
    if not model_list:
        model_list = attrs.get('model_number', [])
    if not model_list:
        model_list = attrs.get('model_name', [])
    if not model_list:
        return ''
    for item in model_list:
        if isinstance(item, dict) and item.get('value'):
            return item['value']
    if isinstance(model_list, list) and len(model_list) > 0 and isinstance(model_list[0], str):
        return model_list[0]
    return ''


def _generate_model(declare_name_en, dimensions_cm, listing_model=''):
    """仅新产品没有平台型号时，按原申报名首字母和尺寸生成首次型号。"""
    if listing_model:
        return listing_model
    if not declare_name_en:
        return ''
    words = declare_name_en.split()
    initials = ''.join(w[0].upper() for w in words if w)
    if not initials:
        return ''
    if dimensions_cm:
        return f"{initials}-{dimensions_cm}"
    return initials


# 只允许这些固定来源字段覆盖已有产品；本地中文名/申报资料/材质/用途/HS不在更新白名单。
FIXED_FIELDS=('brand','declare_value','currency','asin','fnsku','image_url','image_urls','weight_kg','dimensions_cm','sales_url')
PRODUCT_FIELDS=('seller_sku','product_name','brand','declare_name_cn','declare_name_en','material_cn','material_en',
                'purpose','model','hs_code','declare_value','currency','asin','fnsku','image_url','image_urls',
                'weight_kg','dimensions_cm','sales_url','status')


def _shared_declaration(product):
    """仅把已有有效申报名作为同组新产品的共用资料，不改写任何已有子体。"""
    if not product.get('declare_name_cn') and not product.get('declare_name_en'):
        return None
    return {'name_cn':product.get('declare_name_cn') or '', 'name_en':product.get('declare_name_en') or '',
            **{key:product.get(key) or '' for key in ('material_cn','material_en','hs_code','purpose')}}


def _read_product(sku):
    """生成资料之前先检查全局SKU产品；只读连接及时关闭，不持锁等待AI。"""
    conn=get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT * FROM products WHERE seller_sku=%s',(sku,))
            return cursor.fetchone()
    finally:
        conn.close()


def _save_product(sku, fixed, creation):
    """短事务保存固定字段；并发创建撞唯一SKU时也绝不覆盖已落库的AI或人工资料。"""
    conn=get_db_connection()
    try:
        with conn.cursor() as cursor:
            columns=[*FIXED_FIELDS]+(['model'] if fixed.get('model') else [])
            if creation is None:
                cursor.execute('SELECT * FROM products WHERE seller_sku=%s FOR UPDATE',(sku,))
                product=cursor.fetchone()
                if not product:
                    raise ValueError('本地产品已被删除，请刷新后重新确认同步')
                cursor.execute('UPDATE products SET '+','.join(key+'=%s' for key in columns)+',updated_at=NOW() WHERE id=%s',
                               tuple(fixed[key] for key in columns)+(product['id'],))
                action='updated'
            else:
                # 列名来自常量白名单；VALUES只接收本次Listing固定字段，不覆盖新创建的申报内容。
                updates=','.join(key+'=VALUES('+key+')' for key in columns)+',updated_at=NOW()'
                cursor.execute('INSERT INTO products ('+','.join(PRODUCT_FIELDS)+') VALUES ('+
                               ','.join(['%s']*len(PRODUCT_FIELDS))+') ON DUPLICATE KEY UPDATE '+updates,
                               tuple(creation.get(key) for key in PRODUCT_FIELDS))
                action='created' if cursor.rowcount==1 else 'updated'
                cursor.execute('SELECT * FROM products WHERE seller_sku=%s',(sku,))
                product=cursor.fetchone()
        conn.commit()
        return {'status':'success','message':f"产品 {sku} 已创建" if action=='created' else f"产品 {sku} 已更新Listing固定资料，保留本地名称与申报内容",
                'data':{'id':product['id'],'action':action,'preserved_local_content':action=='updated'},
                'decl_info':_shared_declaration(product)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def sync_from_listing(sku, listing, decl_info=None):
    """已有产品只更新Listing固定字段且不调用AI；新产品才首次生成或复用同组申报资料。"""
    if not listing:
        return {'status':'error','message':f'Listing {sku} 不存在'}
    attrs=listing.get('attributes_json') or {}
    try:
        attrs=json.loads(attrs) if isinstance(attrs,str) else attrs
    except (json.JSONDecodeError,TypeError):
        attrs={}
    if not isinstance(attrs,dict):
        attrs={}
    dimensions_cm=_parse_dimensions_cm(attrs)
    listing_model=_extract_model_from_attrs(attrs)
    brand=listing.get('brand') or ''
    if brand.strip().lower()=='generic':
        brand='无'
    image_urls=[image['media_location'] for image in listing.get('images') or []
                if image.get('media_location') and image.get('image_type')!='main']
    asin=(listing.get('asin') or '').strip()
    fixed={'brand':brand,'declare_value':listing.get('list_price'),'currency':listing.get('list_price_currency','USD'),
           'asin':asin,'fnsku':listing.get('fn_sku') or '','image_url':listing.get('main_image_url') or '',
           'image_urls':json.dumps(image_urls,ensure_ascii=False) if image_urls else None,
           'weight_kg':_parse_weight_kg(attrs),'dimensions_cm':dimensions_cm,
           'sales_url':f'https://www.amazon.com/dp/{asin}' if asin else ''}
    if listing_model:
        fixed['model']=listing_model
    if _read_product(sku):
        return _save_product(sku,fixed,None)

    # AI只参与首次创建；没有固定平台型号时，沿用原申报名首字母+尺寸生成型号的规则。
    declaration=decl_info or generate_declaration_info(listing.get('product_description') or '',_extract_material_cn(attrs))
    creation={**fixed,'seller_sku':sku,'product_name':declaration.get('name_cn') or '',
              'declare_name_cn':declaration.get('name_cn') or '','declare_name_en':declaration.get('name_en') or '',
              'material_cn':declaration.get('material_cn') or _extract_material_cn(attrs),
              **{key:declaration.get(key) or '' for key in ('material_en','hs_code','purpose')},
              'model':_generate_model(declaration.get('name_en') or '',dimensions_cm,listing_model),'status':1}
    return _save_product(sku,fixed,creation)


def _parse_weight_kg(attrs):
    """从 attributes 中提取重量并转换为 kg"""
    item_weight = attrs.get('item_weight') or attrs.get('item_package_weight')
    if item_weight and isinstance(item_weight, list) and len(item_weight) > 0:
        w = item_weight[0]
        if isinstance(w, dict):
            value = w.get('value')
            unit = (w.get('unit') or '').lower()
            if value is not None:
                value = float(value)
                if unit == 'pounds':
                    return round(value * 0.453592, 3)
                elif unit == 'ounces':
                    return round(value * 0.028349523125, 3)
                elif unit == 'grams':
                    return round(value / 1000, 3)
                elif unit == 'kg':
                    return round(value, 3)
                return value
    return None


def _extract_material_cn(attrs):
    """从 attributes_json 解析出的 dict 中提取中文材质，多个用、连接"""
    material_list = attrs.get('material', [])
    if not material_list:
        return ""
    values = [item.get('value', '') for item in material_list if item.get('language_tag') == 'zh_CN']
    return "、".join(values)


def _parse_dimensions_cm(attrs):
    """从 attributes 中提取长宽高并转换为 cm，返回 'L*W*H' 格式字符串"""
    dims = attrs.get('item_length_width_height') or attrs.get('item_package_dimensions')
    if dims and isinstance(dims, list) and len(dims) > 0:
        d = dims[0]
        if isinstance(d, dict):
            length = _get_dim_value(d.get('length'))
            width = _get_dim_value(d.get('width'))
            height = _get_dim_value(d.get('height'))
            if length and width and height:
                return f"{length}*{width}*{height}"
    return None


def _get_dim_value(dim):
    """解析单维度值，统一转换为 cm"""
    if not isinstance(dim, dict):
        return None
    value = dim.get('value')
    unit = (dim.get('unit') or '').lower()
    if value is not None:
        value = float(value)
        if unit == 'inches':
            return round(value * 2.54, 2)
        elif unit == 'cm':
            return round(value, 2)
        return value
    return None

"""Listing图片定向编辑及来源区分，不以Amazon域名或旧目录图冒充处理成功。"""
import copy
import json
import base64
import io
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from PIL import Image

from services import listing_pricing_service as pricing
from services import oss_uploader
from services import listing_history_service as history

POSITIONS = ['MAIN'] + [f'PT{i:02d}' for i in range(1, 9)]
ATTRIBUTES = {'MAIN': 'main_product_image_locator', **{f'PT{i:02d}': f'other_product_image_locator_{i}' for i in range(1, 9)}}


def signer(client, purpose):
    value = pricing.signer(client)
    value.salt = ('listing-images-' + purpose + '-v1').encode()
    return value


def upload_proof(client, ctx, asset):
    return signer(client, 'upload').dumps({'context': ctx, 'url': asset['url'], 'oss_key': asset['oss_key'],
                                         'preview_url': asset['preview_url'], 'sha256': asset['sha256'], 'signature': asset['signature']})


def preview_url(url):
    """管理目录小图优先；旧OSS原图仅按需打开，绝不把目录旧图替换进提交草稿。"""
    parsed = urlparse(url)
    own = urlparse(oss_uploader._build_url(''))
    if parsed.scheme == 'https' and parsed.netloc == own.netloc:
        if '/amazon/listing/images/managed/' in parsed.path and parsed.path.endswith(('/original.jpg', '/original.png')):
            return url.rsplit('/', 1)[0] + '/preview.jpg'
        return None
    if parsed.scheme == 'https':
        return url
    return None


def read(client, ctx):
    raw = client.get_listings_item(ctx['sku'], included_data=['summaries', 'attributes', 'issues'])
    if not isinstance(raw, dict) or raw.get('sku') != ctx['sku']:
        raise pricing.PricingError('平台返回SKU不一致，未执行图片修改', 502)
    summaries = [row for row in raw.get('summaries', []) if row.get('marketplaceId') == ctx['marketplace_id']]
    if len(summaries) != 1 or not summaries[0].get('productType'):
        raise pricing.PricingError('无法确认当前站点商品类型，未执行图片修改', 409)
    attrs = raw.get('attributes') or {}
    slots = []
    for position in POSITIONS:
        values = attrs.get(ATTRIBUTES[position]) or []
        if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
            raise pricing.PricingError('平台图片结构异常，请在Amazon核对', 409)
        current = [copy.deepcopy(row) for row in values if row.get('marketplace_id') == ctx['marketplace_id']]
        editable = len(current) <= 1 and all(set(row) <= {'marketplace_id', 'media_location'} and isinstance(row.get('media_location'), str) for row in current) and all(row.get('marketplace_id') for row in values)
        url = current[0].get('media_location') if len(current) == 1 else None
        slots.append({'position': position, 'url': url, 'preview_url': preview_url(url) if url else None,
                      'rows': current, 'editable': bool(editable)})
    snapshot = {**ctx, 'asin': summaries[0].get('asin'), 'product_type': summaries[0]['productType'],
                'slots': slots, 'issues': pricing.safe_issues(raw.get('issues')), 'listing_status': summaries[0].get('status', []),
                'main_image': summaries[0].get('mainImage'), 'fetched_at': datetime.now(timezone.utc).isoformat()}
    snapshot['version'] = pricing.digest([ctx, snapshot['product_type'], {name: attrs.get(name) for name in ATTRIBUTES.values()}])
    return snapshot, raw


def get_editor(shop_id, sku):
    client, ctx = pricing.account(shop_id, sku)
    snapshot, _ = read(client, ctx)
    return snapshot


def build_patches(client, ctx, snapshot, data):
    if data.get('version') != snapshot['version']:
        raise pricing.PricingError('平台图片已变化，请重新读取后调整，未提交本次修改', 409)
    changes = data.get('changes')
    if not isinstance(changes, dict) or not changes or set(changes) - set(POSITIONS):
        raise pricing.PricingError('图片变更位置无效')
    by_position = {row['position']: row for row in snapshot['slots']}
    existing = {row['url'] for row in snapshot['slots'] if row['url']}
    patches, expected, previous, signatures = [], {}, {}, {}
    for position, action in changes.items():
        old = by_position[position]
        if not old['editable']:
            raise pricing.PricingError(f'{position}是复杂配置，本版只读', 409)
        if not isinstance(action, dict) or action.get('mode') not in ('set', 'clear'):
            raise pricing.PricingError('图片操作无效')
        if action['mode'] == 'clear':
            if position == 'MAIN':
                raise pricing.PricingError('不能清空主图，请用替换或设为主图')
            if not old['rows']:
                continue
            patch = {'op': 'delete', 'path': '/attributes/' + ATTRIBUTES[position], 'value': old['rows']}
            url = None
        else:
            url = action.get('url')
            if not isinstance(url, str) or len(url) > 4000 or urlparse(url).scheme != 'https':
                raise pricing.PricingError('图片必须使用有效HTTPS地址')
            if url not in existing:
                try:
                    asset = signer(client, 'upload').loads(action.get('asset_token'), max_age=86400)
                except (pricing.BadSignature, pricing.SignatureExpired, TypeError):
                    raise pricing.PricingError('新图片上传凭据无效或超过24小时，请重新核对，未提交修改', 409) from None
                if asset.get('context') != ctx or asset.get('url') != url:
                    raise pricing.PricingError('图片不属于当前店铺、站点或SKU', 409)
                signatures[position] = asset.get('signature')
            if url == old['url']:
                continue
            patch = {'op': 'replace', 'path': '/attributes/' + ATTRIBUTES[position],
                     'value': [{'media_location': url, 'marketplace_id': ctx['marketplace_id']}]}
        patches.append(patch)
        expected[position], previous[position] = url, old['url']
    if not patches:
        raise pricing.PricingError('图片没有实际变化，无需提交')
    return patches, expected, previous, signatures


def submit_editor(shop_id, sku, data):
    client, ctx = pricing.account(shop_id, sku)
    with pricing.price_lock({**ctx, 'purpose': 'images'}):
        snapshot, _ = read(client, ctx)
        patches, expected, previous, signatures = build_patches(client, ctx, snapshot, data)
        proof = {'context': ctx, 'expected': expected, 'previous': previous, 'signatures': signatures, 'content_matches': {},
                 'baseline_errors': [pricing.digest(row) for row in snapshot['issues'] if row.get('severity') == 'ERROR']}
        try:
            response = client.patch_listings_item(sku, patches, product_type=snapshot['product_type'])
        except requests.RequestException as exc:
            response = getattr(exc, 'response', None)
            if response is not None and response.status_code in (400, 401, 403, 404, 413, 415, 422, 429):
                try: errors = response.json().get('errors', [])
                except (ValueError, AttributeError): errors = []
                issues = [{'code': str(row.get('code', ''))[:100], 'message': str(row.get('message', ''))[:1500], 'severity': 'ERROR'}
                          for row in errors if isinstance(row, dict)][:20] if isinstance(errors, list) else []
                raise pricing.PricingError('Amazon明确拒绝图片提交，请核对问题详情', 422,
                                           {'write_state': 'rejected', 'issues': issues}) from None
            raise pricing.PricingError('图片提交未收到可靠回执，结果待核实，不要重复提交', 502, {'write_state': 'unknown'}) from None
        submission = pricing.accepted_submission(response)
        proof['submission_id'] = submission['submission_id']
        proof['history_id'] = submission.get('history_id')
        return {'state': 'submitted', 'submission': submission,
                'verification_token': signer(client, 'verify').dumps(proof), 'message': 'Amazon已接受图片提交，等待回读；未确认商城展示'}


def compare_amazon_image(client, url, signature):
    """只下载变化后的Amazon候选图，最多2MB；不回源读取OSS原图。"""
    parsed = urlparse(url)
    if parsed.scheme != 'https' or parsed.port not in (None, 443) or not (parsed.hostname or '').endswith(('.media-amazon.com', '.ssl-images-amazon.com')):
        return None
    try:
        with requests.get(url, stream=True, timeout=(5, 15), allow_redirects=False, proxies=client.proxies) as response:
            response.raise_for_status()
            if response.is_redirect: return None
            chunks, size = [], 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > 2 * 1024 * 1024: return None
                chunks.append(chunk)
        with Image.open(io.BytesIO(b''.join(chunks))) as image:
            if image.width * image.height > 40000000: return None
            current = oss_uploader.image_signature(image)
        first, second = base64.b64decode(signature['colors']), base64.b64decode(current['colors'])
        if len(first) != 192 or len(second) != 192: return None
        difference = sum(abs(a - b) for a, b in zip(first, second)) / 192
        distance = bin(int(signature['dhash'], 16) ^ int(current['dhash'], 16)).count('1')
        return {'url': url, 'matched': difference <= 5 and distance <= 4,
                'mean_rgb_difference': round(difference, 3), 'dhash_distance': distance,
                'basis': 'system_visual_comparison_not_amazon_status'}
    except Exception:
        return None


def verify_editor(shop_id, sku, token):
    client, ctx = pricing.account(shop_id, sku)
    try: proof = signer(client, 'verify').loads(token, max_age=86400)
    except (pricing.BadSignature, pricing.SignatureExpired, TypeError):
        raise pricing.PricingError('图片核对凭据无效或超过24小时，请只读重查，勿据此重复提交', 409) from None
    if proof.get('context') != ctx:
        raise pricing.PricingError('图片核对凭据不属于当前店铺或SKU', 409)
    snapshot, raw = read(client, ctx)
    slots = {row['position']: row for row in snapshot['slots']}
    actual = {position: row['url'] for position, row in slots.items()}
    # 多实例等复杂结构不能因url为空而被当作“清除成功”。
    matches = {position: slots[position]['editable'] and
               (not slots[position]['rows'] if url is None else actual[position] == url)
               for position, url in proof['expected'].items()}
    compared = proof.setdefault('content_matches', {})
    budget = 3
    for position, signature in proof.get('signatures', {}).items():
        url = actual[position]
        old_match = compared.get(position)
        if matches[position] or not url or url == proof['previous'][position]:
            compared.pop(position, None)
            continue
        if old_match and old_match['url'] == url: continue
        compared.pop(position, None)
        if budget <= 0 or not signature: continue
        budget -= 1
        comparison = compare_amazon_image(client, url, signature)
        if comparison: compared[position] = comparison
    attributes = {ATTRIBUTES[position] for position in proof['expected']}
    errors = [row for row in snapshot['issues'] if row.get('severity') == 'ERROR' and
              (pricing.digest(row) not in proof['baseline_errors'] or set(row.get('attributeNames') or []) & attributes)]
    state = 'issues' if errors else 'attributes_confirmed' if all(matches.values()) else 'pending'
    if not errors and not all(matches.values()) and all(matches[position] or compared.get(position, {}).get('matched') for position in matches):
        state = 'image_matched'
    messages = {'issues': '平台返回图片处理问题，请核对详情；不会自动重发',
                'attributes_confirmed': '本次图片链接已回读确认；不代表Amazon已完成图片处理或商城展示',
                'image_matched': 'Amazon地址图片与本次上传相似匹配（系统比对）；可用于预览，未确认商城展示，回读仍可能变化',
                'pending': 'Amazon已接受提交，但对应图片链接尚未确认；旧Amazon地址也不能当作新图完成'}
    result = {'state': state, 'message': messages[state], 'snapshot': snapshot, 'issues': errors,
              'submission_id': proof['submission_id'], 'positions': [
                  {'position': position, 'expected_url': url, 'actual_url': actual[position],
                   'matched': matches[position], 'content_comparison': compared.get(position),
                   'changed_from_previous': actual[position] != proof['previous'][position]}
                  for position, url in proof['expected'].items()]}
    result['verification_token'] = signer(client, 'verify').dumps(proof)
    history.confirm(ctx, result, proof.get('history_id'))
    try: cache_images(ctx, raw)
    except Exception: result['cache_warning'] = '平台回读已完成，但本地图片缓存更新失败，请稍后重读，勿重新提交'
    return result


def cache_images(ctx, raw):
    connection = pricing.get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT attributes_json FROM amazon_listings WHERE shop_id=%s AND sku=%s AND marketplace_id=%s FOR UPDATE',
                           (ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            row = cursor.fetchone()
            if not row: return
            attrs = json.loads(row['attributes_json'] or '{}')
            for name in ATTRIBUTES.values():
                if name in raw['attributes']: attrs[name] = raw['attributes'][name]
                else: attrs.pop(name, None)
            cursor.execute('UPDATE amazon_listings SET attributes_json=%s WHERE shop_id=%s AND sku=%s AND marketplace_id=%s',
                           (json.dumps(attrs, ensure_ascii=False), ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            summaries = [item for item in raw.get('summaries', []) if item.get('marketplaceId') == ctx['marketplace_id']]
            main_image = (summaries[0].get('mainImage') or {}) if len(summaries) == 1 else {}
            if main_image.get('link'):
                # 只使用实时摘要回读的主图，不以刚上传的OSS地址乐观覆盖列表缩略图。
                cursor.execute('UPDATE amazon_listings SET main_image_url=%s,main_image_height=%s,main_image_width=%s WHERE shop_id=%s AND sku=%s AND marketplace_id=%s',
                               (main_image['link'], main_image.get('height'), main_image.get('width'), ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            cursor.execute("DELETE FROM amazon_listing_images WHERE shop_id=%s AND sku=%s AND marketplace_id=%s AND image_type IN ('main','other_1','other_2','other_3','other_4','other_5','other_6','other_7','other_8')",
                           (ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            for index, position in enumerate(POSITIONS):
                rows = [item for item in raw['attributes'].get(ATTRIBUTES[position], []) if item.get('marketplace_id') == ctx['marketplace_id']]
                if len(rows) != 1 or not rows[0].get('media_location'): continue
                cursor.execute('INSERT INTO amazon_listing_images (shop_id,sku,marketplace_id,image_type,media_location,sort_order) VALUES (%s,%s,%s,%s,%s,%s)',
                               (ctx['shop_id'], ctx['sku'], ctx['marketplace_id'], 'main' if index == 0 else f'other_{index}', rows[0]['media_location'], index))
        connection.commit()
    except Exception:
        connection.rollback(); raise
    finally:
        connection.close()


def get_catalog(shop_id, sku):
    client, ctx = pricing.account(shop_id, sku)
    snapshot, _ = read(client, ctx)
    if not snapshot['asin']: raise pricing.PricingError('当前Listing没有ASIN，无法核对目录图', 409)
    try:
        raw = client.get_catalog_item(snapshot['asin'], included_data=['images'])
    except requests.RequestException as exc:
        if exc.response is not None and exc.response.status_code in (403, 404):
            return {'available': False, 'images': [], 'message': '当前目录不存在或无权限获取；不能据此判定图片提交失败'}
        raise
    if raw.get('asin') != snapshot['asin']:
        raise pricing.PricingError('平台返回ASIN不一致，未展示目录图', 502)
    grouped = {}
    for group in raw.get('images', []):
        if group.get('marketplaceId') != ctx['marketplace_id']: continue
        for image in group.get('images', []):
            position = image.get('variant')
            if position not in POSITIONS or not image.get('link', '').startswith('https://'): continue
            if position not in grouped or image.get('width', 0) * image.get('height', 0) > grouped[position].get('width', 0) * grouped[position].get('height', 0):
                grouped[position] = image
    return {'available': True, 'images': [grouped[position] for position in POSITIONS if position in grouped],
            'message': 'Amazon目录当前返回的图片，仅供对照；不等于本次提交已生效或所有顾客实时展示'}

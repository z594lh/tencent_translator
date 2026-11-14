"""浏览器直传授权与可信确认；客户端不能直接获得Listing修改凭据。"""
import base64
import hashlib
import hmac
import json
import re
import time
from datetime import datetime, timedelta, timezone

import oss2

from services import oss_uploader as storage
from services import listing_image_service as editor
from services import listing_pricing_service as pricing

MAX_SIZE = 10 * 1024 * 1024
TICKET_SECONDS = 600


def _bucket():
    return storage._get_bucket(secure=True, connect_timeout=(5, 12))


def _paths(ctx, digest, mime):
    # 同一SKU内跨位置复用；不同店铺/站点/SKU保持隔离。
    root = storage.listing_image_prefix(ctx['shop_id'], ctx['marketplace_id'], ctx['sku'], 'MAIN')[:-5]
    directory = root + 'assets/v1/' + digest + '/'
    extension = '.jpg' if mime == 'image/jpeg' else '.png'
    return directory + 'original' + extension, directory + 'preview.jpg', directory + 'verified.json'


def _head(bucket, key):
    try:
        return bucket.head_object(key)
    except oss2.exceptions.NoSuchKey:
        return None


def _read(bucket, key, limit, timeout=30):
    response = bucket.get_object(key)
    chunks, size, started = [], 0, time.monotonic()
    try:
        while True:
            if time.monotonic() - started > timeout:
                raise pricing.PricingError('图片校验暂时超时，重试会复用已上传原图，无需重复上传', 504)
            chunk = response.read(min(65536, limit + 1 - size))
            if not chunk:
                return b''.join(chunks)
            chunks.append(chunk)
            size += len(chunk)
            if size > limit:
                raise pricing.PricingError('OSS返回文件超过预期大小，未确认图片', 409)
    finally:
        response.close()


def _cached(bucket, marker, digest):
    try:
        data = json.loads(_read(bucket, marker, 32768))
    except oss2.exceptions.NoSuchKey:
        return None
    if (not isinstance(data, dict) or data.get('version') != 1 or data.get('sha256') != digest
            or not isinstance(data.get('signature'), dict)):
        raise pricing.PricingError('图片校验记录异常，请联系管理员核对', 409)
    return data


def _asset(client, ctx, request_data, cached, reused):
    key, preview, _ = _paths(ctx, request_data['sha256'], request_data['mime'])
    if cached.get('size') != request_data['size'] or cached.get('mime') != request_data['mime']:
        raise pricing.PricingError('图片内容与已有校验记录不一致', 409)
    asset = {k: cached[k] for k in ('sha256', 'width', 'height', 'size', 'preview_size', 'signature')}
    asset.update(success=True, url=storage._build_url(key), preview_url=storage._build_url(preview),
                 oss_key=key, filename=request_data['filename'], position=request_data['position'], reused=reused)
    asset['asset_token'] = editor.upload_proof(client, ctx, asset)
    return asset


def _manifest(data):
    if not isinstance(data, dict):
        raise pricing.PricingError('上传参数须为对象')
    digest, size, mime = data.get('sha256'), data.get('size'), data.get('mime')
    if not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest):
        raise pricing.PricingError('图片内容指纹无效')
    if type(size) is not int or not 0 < size <= MAX_SIZE or mime not in ('image/jpeg', 'image/png'):
        raise pricing.PricingError('仅支持不超过10MB的JPEG/PNG图片')
    if data.get('position') not in editor.POSITIONS:
        raise pricing.PricingError('图片位置无效')
    filename = data.get('filename')
    if not isinstance(filename, str) or not filename or len(filename) > 255:
        raise pricing.PricingError('图片文件名无效')
    return {k: data[k] for k in ('sha256', 'size', 'mime', 'position', 'filename')}


def prepare(shop_id, sku, data):
    manifest = _manifest(data)
    client, ctx = pricing.account(shop_id, sku)
    bucket = _bucket()
    try:
        key, _, marker = _paths(ctx, manifest['sha256'], manifest['mime'])
        cached = _cached(bucket, marker, manifest['sha256'])
        if cached:
            return {'ready': True, 'asset': _asset(client, ctx, manifest, cached, True)}
        ticket = editor.signer(client, 'direct-upload').dumps({'context': ctx, 'file': manifest})
        if _head(bucket, key):
            return {'ready': False, 'uploaded': True, 'ticket': ticket}
        fields = {'key': key, 'Content-Type': manifest['mime'], 'success_action_status': '204',
                  'x-oss-forbid-overwrite': 'true'}
        policy = {'expiration': (datetime.now(timezone.utc) + timedelta(seconds=TICKET_SECONDS)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  'conditions': [{'bucket': storage.ALI_BUCKET_NAME},
                                 ['content-length-range', manifest['size'], manifest['size']],
                                 *[{k: v} for k, v in fields.items()]]}
        encoded = base64.b64encode(json.dumps(policy, separators=(',', ':')).encode()).decode()
        signature = base64.b64encode(hmac.new(storage.ALI_ACCESS_KEY_SECRET.encode(), encoded.encode(), hashlib.sha1).digest()).decode()
        fields.update(policy=encoded, Signature=signature, OSSAccessKeyId=storage.ALI_ACCESS_KEY_ID)
        return {'ready': False, 'uploaded': False, 'ticket': ticket, 'expires_in': TICKET_SECONDS,
                'upload': {'url': storage._build_url(''), 'fields': fields}}
    finally:
        bucket.session.session.close()


def _put_once(bucket, key, data, mime):
    try:
        bucket.put_object(key, data, headers={'Content-Type': mime,
                          'Cache-Control': 'public, max-age=31536000, immutable', 'x-oss-forbid-overwrite': 'true'})
    except oss2.exceptions.ServerError as exc:
        # 同一内容并发确认时只允许复用已存在的服务端产物，不覆盖。
        if exc.status != 409 or exc.code != 'FileAlreadyExists':
            raise


def complete(shop_id, sku, ticket):
    client, ctx = pricing.account(shop_id, sku)
    try:
        proof = editor.signer(client, 'direct-upload').loads(ticket, max_age=TICKET_SECONDS)
    except (pricing.BadSignature, pricing.SignatureExpired, TypeError):
        raise pricing.PricingError('直传凭据无效或已过期，请重试；已有图片会自动核对复用', 409) from None
    if not isinstance(proof, dict) or proof.get('context') != ctx:
        raise pricing.PricingError('上传图片不属于当前店铺、站点或SKU', 409)
    manifest = _manifest(proof.get('file'))
    bucket = _bucket()
    try:
        key, preview, marker = _paths(ctx, manifest['sha256'], manifest['mime'])
        cached = _cached(bucket, marker, manifest['sha256'])
        if cached:
            return _asset(client, ctx, manifest, cached, True)
        head = _head(bucket, key)
        if head is None:
            raise pricing.PricingError('原图尚未上传完成，请重试上传', 409)
        if head.content_length != manifest['size']:
            raise pricing.PricingError('实际图片大小与上传声明不一致，未确认图片', 409)
        raw = _read(bucket, key, manifest['size'])
        prepared = storage.prepare_listing_image(raw)
        if prepared['sha256'] != manifest['sha256'] or prepared['mime'] != manifest['mime']:
            raise pricing.PricingError('实际图片内容与指纹或格式不一致，未确认图片，请联系管理员核对', 409)
        _put_once(bucket, preview, prepared['preview'], 'image/jpeg')
        cached = {k: prepared[k] for k in ('sha256', 'mime', 'width', 'height', 'signature')}
        cached.update(version=1, size=len(raw), preview_size=len(prepared['preview']))
        _put_once(bucket, marker, json.dumps(cached, separators=(',', ':')).encode(), 'application/json')
        return _asset(client, ctx, manifest, cached, False)
    finally:
        bucket.session.session.close()

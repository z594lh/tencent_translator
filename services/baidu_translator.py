"""百度开放平台文本、分段批量、图片及HTML翻译；不自动重试或回退腾讯。"""
import base64
import binascii
import configparser
import hashlib
import io
import os
import re
import secrets
import threading
import time
from functools import lru_cache
from html import escape
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Comment, NavigableString
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
TEXT_URL = 'https://fanyi-api.baidu.com/api/trans/vip/translate'
IMAGE_URL = 'https://fanyi-api.baidu.com/api/trans/sdk/picture'
QUERY_BYTES = 5800
INPUT_BYTES = 60000
IMAGE_BYTES = 4 * 1024 * 1024
LANGUAGE_ALIASES = {'ja':'jp','ko':'kor','fr':'fra','es':'spa','ar':'ara','vi':'vie',
                    'ms':'may','sv':'swe','ro':'rom','sl':'slo','bg':'bul','et':'est',
                    'da':'dan','zh-cn':'zh','zh-tw':'cht'}


class TranslationError(ValueError):
    def __init__(self, message, status=400, provider_code=None, retry_after=None):
        super().__init__(message)
        self.status = status
        self.provider_code = provider_code
        self.retry_after = retry_after
        self.request_sent = False


def language(value, target=False):
    if not isinstance(value, str):
        raise TranslationError('翻译语种必须为字符串')
    value = LANGUAGE_ALIASES.get(value.strip().lower(), value.strip().lower())
    if not re.fullmatch(r'[a-z]{2,12}', value) or (target and value == 'auto'):
        raise TranslationError('翻译语种无效，目标语种不能为自动检测')
    return value


def read_config():
    """环境变量优先；同一对凭据不能混合来自环境和配置文件。"""
    load_dotenv(BASE_DIR / '.env', override=False)
    appid = os.getenv('BAIDU_TRANSLATE_APPID')
    appkey = os.getenv('BAIDU_TRANSLATE_APPKEY')
    if appid is None and appkey is None:
        config = configparser.ConfigParser(interpolation=None)
        try:
            config.read(BASE_DIR / 'config.ini', encoding='utf-8')
            appid = config.get('BaiduTranslate', 'appid', fallback='')
            appkey = config.get('BaiduTranslate', 'appkey', fallback='')
        except (OSError, configparser.Error, UnicodeError):
            raise TranslationError('百度翻译配置无法读取，请联系管理员', 503) from None
    if not appid or not appid.strip() or not appkey or not appkey.strip():
        raise TranslationError('百度翻译尚未配置，请设置APPID和密钥', 503)
    if appid.startswith('INPUT_') or appkey.startswith('INPUT_'):
        raise TranslationError('百度翻译仍是示例配置，请填写实际APPID和密钥', 503)
    return appid.strip(), appkey.strip()


def provider_error(code):
    """仅返回固定说明，不回传上游error_msg、签名、请求或原文。"""
    messages = {
        '52001':'百度翻译请求超时，请稍后重试', '52002':'百度翻译服务暂不可用',
        '52003':'百度翻译未授权，请核对APPID和对应服务是否开通',
        '52010':'百度图片翻译设备授权容量不足', '54000':'百度翻译参数校验失败',
        '54001':'百度翻译签名校验失败，请联系管理员核对密钥',
        '54003':'百度翻译调用频率受限，请稍后重试',
        '54004':'百度翻译账户余额或可用额度不足，请核对控制台',
        '54005':'百度长文本翻译过于频繁，请稍后重试',
        '58000':'百度翻译IP校验失败，请联系管理员', '58001':'百度翻译不支持当前语种方向',
        '58002':'百度翻译服务已关闭，请核对控制台', '58003':'百度翻译IP被限制，请核对控制台',
        '90107':'百度翻译账号认证尚未通过或生效',
        '69001':'百度图片翻译图片数据有误', '69002':'百度图片识别超时，请稍后重试',
        '69003':'百度图片内容识别失败', '69004':'图片中没有识别到可翻译的文字',
        '69005':'百度图片翻译图片超过4MB', '69006':'百度图片翻译图片尺寸不符合要求',
        '69007':'百度图片翻译只支持PNG/JPEG图片', '69008':'百度图片翻译设备参数校验失败',
    }
    safe_code = code if re.fullmatch(r'\d{1,8}', code) else None
    status = 429 if code in ('54003','54005') else 400 if code in ('58001','69001','69003','69004','69005','69006','69007') else 502
    return TranslationError(messages.get(code, '百度翻译返回异常，请联系管理员核对服务'), status,
                            safe_code, 3 if code == '54005' else 1 if status == 429 else None)


class BaiduTranslator:
    def __init__(self, appid, appkey):
        self.appid = appid
        self._appkey = appkey
        self._lock = threading.Lock()
        self._next_request = 0.0

    def _sign(self, text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def _request(self, url, data, files=None):
        from services.baidu_rate_limit import account_request
        with account_request(self.appid, len(data.get('q', ''))):
            try:
                return self._send(url, data, files)
            except TranslationError as exc:
                exc.request_sent = True
                raise

    def _send(self, url, data, files=None):
        # 单进程有界串行且至少间隔1秒；多worker总QPS仍由百度账户限制。
        if not self._lock.acquire(timeout=0.1):
            raise TranslationError('翻译服务正忙，请稍后重试', 429, retry_after=1)
        response = None
        try:
            wait = self._next_request - time.monotonic()
            if wait > 3:
                raise TranslationError('翻译服务正在冷却，请稍后重试', 429, retry_after=3)
            if wait > 0:
                time.sleep(wait)
            self._next_request = time.monotonic() + 1.05
            try:
                # 认证参数放请求体，不写URL；拒绝重定向，避免签名/原文被转发。
                response = requests.post(url, data=data, files=files, timeout=(5, 20), allow_redirects=False)
                if response.status_code == 429:
                    self._next_request = time.monotonic() + 3
                    raise TranslationError('百度翻译调用频率受限，请稍后重试', 429, retry_after=3)
                if response.status_code != 200:
                    raise TranslationError('百度翻译HTTP响应异常，请稍后重试', 502)
                result = response.json()
            except requests.Timeout:
                raise TranslationError('百度翻译请求超时，未自动重试', 504) from None
            except (requests.RequestException, ValueError) as exc:
                if isinstance(exc, TranslationError):
                    raise
                raise TranslationError('百度翻译连接或响应异常，未自动重试', 502) from None
            if not isinstance(result, dict):
                raise TranslationError('百度翻译返回格式无效', 502)
            code = str(result.get('error_code', '0'))
            if code not in ('0', '52000'):
                error = provider_error(code)
                if error.retry_after:
                    self._next_request = time.monotonic() + error.retry_after
                raise error
            return result
        finally:
            if response is not None:
                response.close()
            self._lock.release()

    def translate_lines(self, lines, source, target):
        """百度明确支持换行分段；校验返回原文及数量，不能错配到其他关键词。"""
        query = '\n'.join(lines)
        salt = secrets.token_hex(16)
        result = self._request(TEXT_URL, {'appid':self.appid, 'q':query, 'from':source,
            'to':target, 'salt':salt, 'sign':self._sign(self.appid + query + salt + self._appkey)})
        rows = result.get('trans_result')
        if not isinstance(rows, list) or len(rows) != len(lines) or any(
                not isinstance(row, dict) or row.get('src') != original or
                not isinstance(row.get('dst'), str) or not row['dst'].strip()
                for row, original in zip(rows, lines)):
            error = TranslationError('百度翻译分段结果不完整或原文不对应，未使用错位译文', 502)
            error.request_sent = True
            raise error
        return [row['dst'] for row in rows]

    def translate_image(self, image, source, target, mime, extension):
        salt = secrets.token_hex(16)
        image_md5 = hashlib.md5(image).hexdigest()
        data = {'appid':self.appid, 'from':source, 'to':target, 'salt':salt,
                'cuid':'APICUID', 'mac':'mac', 'version':'3', 'paste':'0'}
        data['sign'] = self._sign(self.appid + image_md5 + salt + data['cuid'] + data['mac'] + self._appkey)
        result = self._request(IMAGE_URL, data, {'image':('upload.'+extension, image, mime)})
        payload = result.get('data')
        rows = payload.get('content') if isinstance(payload, dict) else None
        if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) or
                not isinstance(row.get('src'), str) or not isinstance(row.get('dst'), str) for row in rows):
            raise TranslationError('百度图片翻译未返回可用文字', 502)
        # 旧页面以HTML显示图片译文，原文/译文均转义，不能执行图片中的脚本。
        originals = '<br/>'.join('- '+escape(row['src']) for row in rows)
        translated = '<br/>'.join('- '+escape(row['dst']) for row in rows)
        return '原文：<br/>'+originals+'<br/><br/>译文：<br/>'+translated


@lru_cache(maxsize=4)
def _configured_client(appid, appkey):
    return BaiduTranslator(appid, appkey)


def get_client():
    return _configured_client(*read_config())


def _chunks(text):
    """按UTF-8字节限制拆分，不在多字节字符内部截断。"""
    chunks, current, size = [], [], 0
    for char in text:
        length = len(char.encode('utf-8'))
        if size + length > QUERY_BYTES:
            chunks.append(''.join(current))
            current, size = [], 0
        current.append(char)
        size += length
    if current:
        chunks.append(''.join(current))
    return chunks


def translate_batch(texts, from_lang='auto', to_lang='zh', client=None):
    """有界批量、去重并保留空白与顺序；不存储成功或失败译文。"""
    if not isinstance(texts, list) or len(texts) > 100 or any(not isinstance(text, str) for text in texts):
        raise TranslationError('批量翻译须为不超过100项的字符串列表')
    try:
        size = sum(len(text.encode('utf-8')) for text in texts)
    except UnicodeError:
        raise TranslationError('待翻译文本含无效的Unicode字符') from None
    if size > INPUT_BYTES:
        raise TranslationError('本次待翻译文本总长度超过60000字节', 413)
    source, target = language(from_lang), language(to_lang, target=True)
    if source == target:
        return list(texts)
    plans, unique = [], {}
    for text in texts:
        plan = []
        for part in re.split(r'(\r\n|\r|\n)', text):
            for chunk in _chunks(part):
                core = chunk.strip()
                plan.append((chunk, core))
                if core:
                    unique.setdefault(core, None)
        plans.append(plan)
    if unique:
        actual_client = client if client is not None else get_client()
        group, size = [], 0
        for text in unique:
            length = len(text.encode('utf-8')) + (1 if group else 0)
            if group and size + length > QUERY_BYTES:
                unique.update(zip(group, actual_client.translate_lines(group, source, target)))
                group, size = [], 0
            group.append(text)
            size += len(text.encode('utf-8')) + (1 if len(group) > 1 else 0)
        if group:
            unique.update(zip(group, actual_client.translate_lines(group, source, target)))
    return [''.join(chunk if not core else chunk[:len(chunk)-len(chunk.lstrip())] + unique[core] +
                    chunk[len(chunk.rstrip()):] for chunk, core in plan) for plan in plans]


def translate(text, from_lang='auto', to_lang='zh', client=None):
    if not isinstance(text, str):
        raise TranslationError('待翻译内容必须为字符串')
    return translate_batch([text], from_lang, to_lang, client)[0]


def translate_image(image_data, from_lang='auto', to_lang='zh', client=None):
    from PIL import Image, UnidentifiedImageError
    source, target = language(from_lang), language(to_lang, target=True)
    if not isinstance(image_data, str) or not image_data or len(image_data) > (IMAGE_BYTES+2)//3*4+100:
        raise TranslationError('图片数据为空或超过4MB', 413)
    if image_data.startswith('data:'):
        image_data = image_data.split(',', 1)[-1]
    try:
        raw = base64.b64decode(image_data, validate=True)
    except (ValueError, binascii.Error):
        raise TranslationError('图片Base64数据无效') from None
    if not raw or len(raw) > IMAGE_BYTES:
        raise TranslationError('图片数据为空或超过4MB', 413)
    try:
        with Image.open(io.BytesIO(raw)) as image:
            width, height = image.size
            fmt = image.format
            if fmt not in ('PNG','JPEG'):
                raise TranslationError('百度图片翻译仅支持PNG/JPEG图片')
            if min(width,height) < 30 or max(width,height) > 4096 or max(width,height) > min(width,height)*3:
                raise TranslationError('图片短边至少30px，长边最多4096px，长宽比不得超过3:1')
            image.verify()
    except TranslationError:
        raise
    except (UnidentifiedImageError, OSError, ValueError, SyntaxError, Image.DecompressionBombError):
        raise TranslationError('图片数据无法识别或尺寸过大') from None
    actual_client = client if client is not None else get_client()
    return actual_client.translate_image(raw, source, target, 'image/png' if fmt=='PNG' else 'image/jpeg',
                                         'png' if fmt=='PNG' else 'jpg')


def translate_html_with_structure(html, client=None, source='en', target='ms'):
    if not isinstance(html, str):
        raise TranslationError('HTML内容必须为字符串')
    soup = BeautifulSoup(html, 'html.parser')
    nodes = [node for node in soup.find_all(string=True) if not isinstance(node, Comment) and node.strip()
             and not any(parent.name in ('script','style','noscript','template') for parent in node.parents)]
    translated = translate_batch([str(node) for node in nodes], source, target, client)
    for node, value in zip(nodes, translated):
        node.replace_with(NavigableString(value))
    return str(soup)

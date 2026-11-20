"""发给模型的程序文字目录：命名变量替换、任务版本绑定，不执行模板代码。"""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import json
import re
from pathlib import Path
from string import Formatter

CATALOG = json.loads(Path(__file__).with_name('language_catalog.json').read_text(encoding='utf-8'))
_active = ContextVar('ai_operation_language', default=None)


class ConfiguredText(str):
    """模块级常量保留文字来源，资料冻结时用本任务版本重新渲染。"""
    def __new__(cls, value, key, variables):
        instance = super().__new__(cls, value)
        instance.key, instance.variables = key, variables
        return instance

    def __reduce__(self):
        return (ConfiguredText, (str(self), self.key, self.variables))

    def __add__(self, other):
        return str(text(self.key, **self.variables)) + other

    def __radd__(self, other):
        return other + str(text(self.key, **self.variables))


def defaults():
    return {row['key']: row['default'] for row in CATALOG}


def current_config():
    return _active.get()


def variables(template):
    """只允许简单命名变量；禁止属性访问、索引、格式表达式及转换。"""
    found = set()
    for _, key, spec, conversion in Formatter().parse(template):
        if key is None:
            continue
        if not key.isidentifier() or spec or conversion:
            raise ValueError('模板只允许简单命名变量')
        found.add(key)
    return found


def validate(values):
    expected = defaults()
    if not isinstance(values, dict) or set(values) - set(expected):
        raise ValueError('AI文字配置含未知字段')
    result = {**expected, **values}
    for key, value in result.items():
        if not isinstance(value, str) or not value.strip() or len(value) > 6000 or any(ord(c) < 32 and c not in '\n\t' for c in value):
            raise ValueError('AI文字不能为空、超过6000字或包含控制字符：'+key)
        if variables(value) != variables(expected[key]):
            raise ValueError('请保留全部模板变量，且不要新增变量：'+key)
    return result


def text(key, **kwargs):
    """默认文字独立于执行逻辑；仅当前冻结配置可影响任务，不重读实时配置。"""
    config = _active.get()
    template = (config or {}).get('language', {}).get(key, DEFAULTS[key])
    # 变量视为纯数据，值中的括号、SQL或表达式都不会被再次解释。
    value = template.format_map(kwargs)
    return ConfiguredText(value, key, kwargs)


def render_tree(value):
    """只渲染程序显式标记文字；商品文案、用户输入和历史结论原样保留。"""
    if isinstance(value, ConfiguredText):
        return text(value.key, **value.variables)
    if isinstance(value, dict):
        return {key: render_tree(row) for key, row in value.items()}
    if isinstance(value, list):
        return [render_tree(row) for row in value]
    if isinstance(value, tuple):
        return tuple(render_tree(row) for row in value)
    return value


def external_text(value):
    """共享领域服务不依赖AI；只有进入模型的说明字段在此适配版本文字。"""
    if not isinstance(value, str) or isinstance(value, ConfiguredText):
        return value
    for key, pattern in EXTERNAL_PATTERNS:
        match = pattern.fullmatch(value)
        if match:
            return text(key, **match.groupdict())
    # 拼接的多条固定说明分别渲染，新增文案不会在同一轮再次被当作模板解析。
    return EXTERNAL_LITERALS.sub(lambda match: str(text(EXTERNAL_LITERAL_KEYS[match.group()])), value) if EXTERNAL_LITERAL_KEYS else value


def external_tree(value, metadata=False, *, fields=()):
    if isinstance(value, dict):
        result = {}
        for key, row in value.items():
            if key in ('report_fields','raw_row','raw_row_json','question','answer','summary','original_reason','before','after','baseline_snapshot'):
                result[key] = row
            else:
                explanatory = key in ('note','notes','notice','warnings','help','description','message','progress_note','calc_note') or key in fields or isinstance(key, str) and key.endswith('_note')
                result[key] = external_tree(row, explanatory, fields=fields)
        return result
    if isinstance(value, (list,tuple)):
        return [external_tree(row, metadata, fields=fields) for row in value]
    return external_text(value) if metadata else value


@contextmanager
def bind(config):
    token = _active.set(config)
    try:
        yield
    finally:
        _active.reset(token)


def freeze_build(function):
    """资料和提示词固定同一版本；运行中或配置发布均不覆盖既有任务。"""
    @wraps(function)
    def wrapped(*args, **kwargs):
        # 配置服务依赖目录而不依赖资料构建；运行时入口解析以避免模块初始化顺序依赖。
        from . import prompt_settings
        frozen = prompt_settings.snapshot()
        with bind(frozen['config']):
            pack, data = render_tree(function(*args, **kwargs))
            pack, data = external_tree(pack), external_tree(data)
        from .metrics import digest, dumps
        pack['content_hash'] = digest({key: value for key, value in pack.items() if key not in ('content_hash','generated_at','size_bytes')})
        pack['size_bytes'] = len(dumps({key: value for key, value in pack.items() if key != 'size_bytes'}).encode('utf8'))
        data['content_hash'] = digest(data['content'])
        data['size_bytes'] = len(dumps(data['content']).encode('utf8'))
        data['text_snapshot'] = frozen
        return pack, data
    return wrapped


def task_bound(function):
    @wraps(function)
    def wrapped(data, prepared, *args, **kwargs):
        with bind(prepared['prompt_snapshot']['config']):
            return function(data, prepared, *args, **kwargs)
    return wrapped


DEFAULTS = defaults()
EXTERNAL_KEYS = json.loads(Path(__file__).with_name('language_external.json').read_text(encoding='utf-8'))
EXTERNAL_PATTERNS, EXTERNAL_LITERAL_KEYS = [], {}
for _key in EXTERNAL_KEYS:
    _parts = list(Formatter().parse(DEFAULTS[_key]))
    _pattern = ''.join(re.escape(literal) + (f'(?P<{field}>.*?)' if field else '') for literal, field, _, _ in _parts)
    if variables(DEFAULTS[_key]):
        EXTERNAL_PATTERNS.append((_key, re.compile(_pattern, re.DOTALL)))
    else:
        EXTERNAL_LITERAL_KEYS[DEFAULTS[_key].format_map({})] = _key
EXTERNAL_LITERALS = re.compile('|'.join(re.escape(v) for v in sorted(EXTERNAL_LITERAL_KEYS, key=len, reverse=True)))

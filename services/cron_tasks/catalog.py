"""固定任务白名单与只读调度发现；绝不执行解析到的命令文本。"""
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from croniter import croniter

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'scripts' / 'cron' / 'run_task.py'


@dataclass(frozen=True)
class Task:
    id: str
    script: str
    args: tuple
    group: str
    title: str
    scope: str
    manual: bool = True


def _task(key, script, title, scope, *args, manual=True):
    # 每个计划任务独立互斥，手动与自动由同一个task_id确定锁，禁止跨任务共组。
    return Task(key, script, args, 'task_' + key, title, scope, manual)


TASKS = {t.id: t for t in (
    _task('orders_recent', 'orders', '近期订单同步', '全部启用店铺，最近24小时', '--recent'),
    _task('orders_week', 'orders', '本周订单同步', '全部启用店铺，最近7天', '--week'),
    _task('orders_month', 'orders', '本月订单列表同步', '全部启用店铺，最近30天，仅订单列表', '--month'),
    _task('inventory', 'inventory', 'FBA库存同步', '全部启用店铺，当前库存'),
    _task('listing', 'listing', 'Listing同步', '全部启用店铺，按原脚本同步'),
    _task('inbound_recent', 'inbound', '近期入库同步', '全部启用店铺，原30分钟模式', '--30min'),
    _task('inbound_full', 'inbound', '完整入库同步', '全部启用店铺，原6小时模式', '--6h'),
    _task('finances', 'finances', '财务明细同步', '最近30天；本版仅定时执行', manual=False),
    _task('product_fees', 'product_fees', '产品费用同步', '全部启用店铺，按原脚本同步'),
    _task('ads_create', 'ads_create', '创建广告报告', '全部启用店铺，昨日；不强制重建'),
    _task('ads_download', 'ads_download', '下载广告报告', '全部启用店铺，昨日；不强制重建'),
    _task('ads_entity_sync', 'ads_entity_sync', '广告实体同步', '全部启用店铺，只同步实体，不调整广告'),
    _task('ads_report', 'ads_report', '生成广告日报', '昨日广告日报', '--daily'),
    _task('reports', 'reports', '生成经营报表', '最近10天经营日报及原SKU/库存汇总', '--daily'),
    _task('rebuild_reports', 'rebuild_reports', '消费报表重建任务', '处理当前待重建列表，不创建新回填范围'),
    _task('exchange_rate', 'exchange_rate', '更新汇率', '原脚本默认币种'),
    _task('storage_fee_sync', 'storage_fee_sync', '同步月度仓储费', '全部启用店铺，原脚本默认账期'),
    _task('storage_analysis_sync', 'storage_analysis_sync', '采集仓储完整报告', '全部启用店铺；每日库存健康、最近两个完整月的月费/超龄/超量报告及历史在途续查'),
    _task('auto_complete', 'auto_complete', '超期订单自动完结', '会完结采购单/运单并入账；禁止手动执行', manual=False),
)}
GROUPS = sorted({t.group for t in TASKS.values()})
# 普通业务HTTP页面的原锁名，仅兼容既有入口/旧审计；新计划任务不使用这些锁。
LEGACY_HTTP_GROUPS = ('orders', 'inventory', 'listing', 'inbound', 'finances', 'product_fees',
                      'reporting', 'ads_entities', 'exchange_rate', 'storage_fees', 'auto_complete')


def get_task(task_id):
    if task_id not in TASKS:
        raise ValueError('任务不在执行白名单中')
    return TASKS[task_id]


def supported():
    return sys.platform.startswith('linux')


def local_timezone():
    if os.environ.get('TZ'):
        return ZoneInfo(os.environ['TZ'])
    if supported():
        with open('/etc/localtime', 'rb') as stream:
            return ZoneInfo.from_file(stream)
    return datetime.now().astimezone().tzinfo


def parse_command(command):
    """只认完整固定参数；Shell包装、额外参数、错误解释器一律不算已接入。"""
    try:
        # 日志重定向只用于兼容展示，不会成为子进程的输出路径。
        command = re.sub(r'\s+>>\s*(?:"[^"\n]+"|\x27[^\x27\n]+\x27|[^\s;&|<>]+)\s+2>&1\s*$', '', command)
        tokens = shlex.split(command)
        if tokens[:1] == ['cd']:
            if len(tokens) < 4 or tokens[2] != '&&' or Path(tokens[1]).resolve() != ROOT:
                return None, False
            tokens = tokens[3:]
        if len(tokens) > 1 and tokens[1] == '-u':
            tokens.pop(1)
        if len(tokens) == 4 and Path(tokens[1]).is_absolute() and Path(tokens[1]).resolve() == RUNNER:
            if tokens[2] == '--task' and tokens[3] in TASKS:
                same_python = os.path.isabs(tokens[0]) and os.path.abspath(tokens[0]) == os.path.abspath(sys.executable)
                if not same_python and os.path.isabs(tokens[0]):
                    candidate, current = Path(tokens[0]), Path(sys.executable)
                    # 同一虚拟环境中的python/python3符号链接可等价；不能接受其他环境。
                    same_python = (candidate.parent == current.parent and bool(re.fullmatch(r'python(?:3(?:\.\d+)?)?', candidate.name))
                                   and candidate.resolve(strict=True) == current.resolve(strict=True))
                return TASKS[tokens[3]], same_python
        # 旧计划仅识别和展示，永远不由本接口复用其命令字符串。
        for index, token in enumerate(tokens):
            match = re.search(r'(?:^|/)scripts/cron/(\w+)\.py$', token)
            if match:
                for task in TASKS.values():
                    if task.script == match[1] and tuple(tokens[index + 1:]) == task.args:
                        return task, False
        return None, False
    except (ValueError, OSError, IndexError):
        return None, False


def discover():
    if not supported():
        return {'supported': False, 'ready': False, 'message': '当前Windows环境不支持Linux计划任务，请在服务器环境查看和执行。', 'tasks': []}
    try:
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return {'supported': True, 'ready': False, 'message': '无法读取当前服务账户的crontab，请核对服务器配置。', 'tasks': []}
    if result.returncode:
        return {'supported': True, 'ready': False, 'message': '当前服务账户没有可读取的计划任务。', 'tasks': []}
    return parse_crontab(result.stdout)


def parse_crontab(content):
    tasks, comments = [], []
    unsafe = False
    timezone_unknown = False
    tz = local_timezone()
    for number, line in enumerate(content.splitlines(), 1):
        line = line.strip()
        if not line:
            comments = []
            continue
        if line.startswith('#'):
            comments.append(line.lstrip('#').strip())
            continue
        if re.match(r'^\w+\s*=', line):
            key = line.split('=', 1)[0].strip()
            if key not in ('SHELL', 'PATH', 'MAILTO'):
                unsafe = True
            timezone_unknown = timezone_unknown or key in ('TZ', 'CRON_TZ')
            if key == 'SHELL' and line.split('=', 1)[1].strip().strip('\"\x27') not in ('/bin/sh', '/bin/bash'):
                unsafe = True
            comments = []
            continue
        # 备份/日志清理可能也包含项目路径，但不属于业务脚本。
        relevant = 'scripts/cron/' in line or (str(ROOT) in line and '.py' in line)
        if not relevant:
            comments = []
            continue
        parts = line.split(None, 5)
        schedule, command = (' '.join(parts[:5]), parts[5]) if len(parts) == 6 else ('', line)
        task, managed = parse_command(command)
        valid = bool(schedule and croniter.is_valid(schedule))
        managed = managed and valid and '%' not in command
        unsafe = unsafe or not managed
        next_run = croniter(schedule, datetime.now(tz)).get_next(datetime).isoformat() if valid else None
        tasks.append({
            'row_id': number, 'task_id': task.id if task else None,
            'task_name': task.title if task else '未识别的项目任务',
            'command': f'cron/{task.script}.py ' + ' '.join(task.args) if task else '命令未通过白名单校验',
            'schedule': schedule, 'description': ' '.join(comments)[-400:],
            'scope': task.scope if task else '需人工核对命令和参数',
            'managed': bool(managed), 'manual_allowed': bool(task and task.manual),
            'group': task.group if task else None, 'next_run': next_run,
        })
        comments = []
    if timezone_unknown:
        for task in tasks:
            task['next_run'] = None
    ready = bool(tasks) and not unsafe
    return {'supported': True, 'ready': ready, 'message': '' if ready else '尚有任务未接入统一防重入口，或crontab环境需要核对；暂不开放手动执行。', 'tasks': tasks}

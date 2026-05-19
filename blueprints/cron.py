"""
计划任务看板 - 只读展示系统 crontab 中的计划任务
"""
import os
import re
import subprocess
from datetime import datetime
from flask import Blueprint, jsonify
from croniter import croniter

cron_bp = Blueprint('cron', __name__, url_prefix='/api/cron')


def _parse_last_run(log_path, task_name):
    """读取日志文件，提取该任务最近一次执行的时间戳"""
    if not log_path or not os.path.exists(log_path):
        return None

    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 2048), os.SEEK_SET)
            tail = f.read()
    except Exception:
        return None

    ts_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]')

    candidates = []
    for line in tail.splitlines():
        if task_name and task_name not in line:
            continue
        match = ts_pattern.search(line)
        if match:
            try:
                dt = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
                candidates.append(dt)
            except ValueError:
                continue

    return max(candidates) if candidates else None


def _calc_next_run(schedule):
    """根据 cron 表达式计算下次执行时间"""
    try:
        itr = croniter(schedule, datetime.now())
        return itr.get_next(datetime)
    except Exception:
        return None


def parse_crontab():
    """执行 crontab -l，返回与当前项目相关的结构化任务列表"""
    try:
        result = subprocess.run(
            ['crontab', '-l'],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return {'error': f"crontab -l 执行失败: {result.stderr.strip()}", 'tasks': []}
        lines = result.stdout.splitlines()
    except FileNotFoundError:
        return {'error': '当前系统不支持 crontab 命令', 'tasks': []}
    except subprocess.TimeoutExpired:
        return {'error': 'crontab -l 执行超时', 'tasks': []}
    except Exception as e:
        return {'error': f"读取 crontab 异常: {str(e)}", 'tasks': []}

    tasks = []
    pending_comment = []

    cron_pattern = re.compile(
        r'^\s*([\*\d,\-\/]+)\s+'
        r'([\*\d,\-\/]+)\s+'
        r'([\*\d,\-\/]+)\s+'
        r'([\*\d,\-\/]+)\s+'
        r'([\*\d,\-\/]+)\s+'
        r'(.+)$'
    )

    target_script = '/home/root/tencent_translator/scripts/cron_jobs.py'

    for line in lines:
        line = line.rstrip('\n')
        stripped = line.strip()

        if not stripped or stripped.startswith('SHELL=') or stripped.startswith('PATH='):
            pending_comment = []
            continue

        if stripped.startswith('#'):
            comment_text = stripped.lstrip('#').strip()
            if comment_text:
                pending_comment.append(comment_text)
            continue

        match = cron_pattern.match(stripped)
        if match:
            minute, hour, dom, month, dow, command = match.groups()

            if target_script not in command:
                pending_comment = []
                continue

            task_name = ''
            m = re.search(r'cron_jobs\.py\s+(\S+)', command)
            if m:
                task_name = m.group(1)

            log_path = ''
            m = re.search(r'>+\s+(\S+)', command)
            if m:
                log_path = m.group(1)

            display_command = command
            if 'cron_jobs.py' in command:
                parts = command.split()
                for i, p in enumerate(parts):
                    if p.endswith('cron_jobs.py') and i + 1 < len(parts):
                        display_command = f"cron_jobs.py {parts[i + 1]}"
                        break

            raw_desc = ' '.join(pending_comment) if pending_comment else ''
            clean_desc = re.sub(r'=+', '', raw_desc).strip()

            schedule = f"{minute} {hour} {dom} {month} {dow}"

            tasks.append({
                'schedule': schedule,
                'minute': minute,
                'hour': hour,
                'day_of_month': dom,
                'month': month,
                'day_of_week': dow,
                'command': display_command,
                'full_command': command,
                'task_name': task_name,
                'log_path': log_path,
                'description': clean_desc,
                'last_run': _parse_last_run(log_path, task_name),
                'next_run': _calc_next_run(schedule),
            })
            pending_comment = []
        else:
            pending_comment = []

    return {'tasks': tasks}


def _paginate_and_filter(tasks, keyword, page, per_page):
    """对任务列表进行搜索过滤和分页"""
    if keyword:
        filtered = []
        for t in tasks:
            task_name = (t.get('task_name') or '').lower()
            description = (t.get('description') or '').lower()
            if keyword in task_name or keyword in description:
                filtered.append(t)
        tasks = filtered

    total = len(tasks)
    total_pages = (total + per_page - 1) // per_page if total > 0 else 1
    start = (page - 1) * per_page
    end = start + per_page
    paged_tasks = tasks[start:end]

    return paged_tasks, {
        'page': page,
        'per_page': per_page,
        'total': total,
        'total_pages': total_pages,
    }


@cron_bp.route('/tasks', methods=['GET'])
def get_tasks():
    """获取计划任务列表，支持搜索和分页"""
    from flask import request

    keyword = request.args.get('keyword', '').strip().lower()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)

    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 10
    if per_page > 100:
        per_page = 100

    result = parse_crontab()
    if 'error' in result:
        return jsonify({'success': False, 'error': result['error'], 'tasks': result['tasks']}), 500
    tasks = result['tasks']


    paged_tasks, pagination = _paginate_and_filter(tasks, keyword, page, per_page)

    return jsonify({
        'success': True,
        'tasks': paged_tasks,
        'pagination': pagination,
    })

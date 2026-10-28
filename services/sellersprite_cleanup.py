"""清理服务器已完成采集的Excel原件；保留清洗快照、任务/文件元数据及VM原件。

只遍历数据库记录，不扫描删除未知文件或上传临时文件。默认预览，apply=True才删除。
竞品批量文件按实际路径合并引用，任何引用未成功都保护整份原件。
"""
import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from services.mysql_service import get_db_connection
from services.sellersprite_common import storage_root, MAX_FILE_SIZE


def _paths(root, kind, value):
    """仅接受本业务的相对归档结构；拒绝绝对路径、越界及符号链接。"""
    path = Path(str(value or ''))
    parts = path.parts
    legacy = (len(parts) == 2 and re.fullmatch(r'\d{4}-\d{2}-\d{2}', parts[0]))
    current = (len(parts) == 3 and parts[0] == 'keyword'
               and re.fullmatch(r'\d{4}-\d{2}-\d{2}', parts[1]))
    if path.is_absolute() or path.suffix.lower() != '.xlsx' or not (legacy or (kind == 'keyword' and current)):
        raise ValueError('归档路径不在允许结构内')
    base = root if kind == 'keyword' else root / 'competitors'
    values = [base / path]
    if kind == 'keyword' and legacy:
        values.append(base / 'keyword' / path)
    checked = []
    for value in values:
        if any(p.is_symlink() for p in (value, *value.parents) if p != root.parent):
            raise ValueError('符号链接归档不自动清理')
        resolved = value.resolve()
        if not resolved.is_relative_to(root):
            raise ValueError('归档路径越界')
        checked.append(resolved)
    return checked


def load_references():
    """先完整读取两类引用；任何查询失败时尚未开始删除文件。"""
    conn = get_db_connection()
    try:
        result = []
        with conn.cursor() as cur:
            for kind in ('keyword', 'competitor'):
                # 表名取自固定白名单，不接受命令行或HTTP输入。
                data_table = 'sellersprite_keyword_rows' if kind == 'keyword' else 'sellersprite_competitor_products'
                cur.execute(f'''SELECT f.storage_path,f.sha256,f.status AS file_status,
                    f.uploaded_at,f.row_count,t.status AS task_status,
                    (SELECT COUNT(*) FROM {data_table} d WHERE d.file_id=f.id AND d.task_id=f.task_id) AS stored_rows
                    FROM sellersprite_{kind}_files f
                    LEFT JOIN sellersprite_{kind}_tasks t ON t.id=f.task_id''')
                result.extend({**row, 'kind': kind} for row in cur.fetchall())
        return result
    finally:
        conn.close()


def cleanup(*, apply=False, min_age_hours=1):
    """仅删除成功且超过保护期的原件；返回数量，不回传路径、ASIN或凭证。"""
    if not 1 <= min_age_hours <= 8760:
        raise ValueError('保护期必须为1～8760小时')
    root = storage_root().resolve()
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=min_age_hours)
    references = load_references()
    groups = {}
    result = {'mode': 'apply' if apply else 'dry_run', 'references': len(references),
              'eligible_files': 0, 'deleted_files': 0, 'eligible_bytes': 0,
              'missing_files': 0, 'protected_files': 0, 'invalid_records': 0, 'errors': 0}
    # 只要有无法归组的记录便停止，避免遗漏共享文件的一条保护引用。
    for row in references:
        try:
            paths = _paths(root, row['kind'], row['storage_path'])
        except ValueError:
            result['invalid_records'] += 1
            continue
        for path in paths:
            groups.setdefault(path, []).append(row)
    if result['invalid_records']:
        return result
    for path, refs in groups.items():
        if not path.is_file():
            result['missing_files'] += 1
            continue
        if any(r['file_status'] != 'imported' or r['task_status'] != 'completed'
               or not r.get('row_count') or r.get('stored_rows') != r['row_count']
               or not isinstance(r['uploaded_at'], datetime) or r['uploaded_at'] > cutoff for r in refs):
            result['protected_files'] += 1
            continue
        hashes = {r['sha256'] for r in refs}
        if len(hashes) != 1:
            result['protected_files'] += 1
            continue
        try:
            before = path.stat()
            if before.st_size > MAX_FILE_SIZE:
                result['protected_files'] += 1
                continue
            digest = hashlib.sha256()
            with path.open('rb') as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                    digest.update(chunk)
            after = path.stat()
            if (digest.hexdigest() not in hashes or
                    (before.st_ino, before.st_size, before.st_mtime_ns) !=
                    (after.st_ino, after.st_size, after.st_mtime_ns)):
                result['protected_files'] += 1
                continue
            result['eligible_files'] += 1
            result['eligible_bytes'] += after.st_size
            if apply:
                path.unlink()
                result['deleted_files'] += 1
        except FileNotFoundError:
            result['missing_files'] += 1
        except OSError:
            result['errors'] += 1
    return result

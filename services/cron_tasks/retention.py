"""仅清理本执行器过期结束记录及对应UUID日志，不涉及任何业务数据。"""
from datetime import datetime, timedelta
import re
import stat

from services.cron_tasks import repository as repo
from services.cron_tasks.locking import safe_directory


def prune_history():
    cutoff = datetime.utcnow() - timedelta(days=7)
    candidates = repo.expired_runs(cutoff)
    directory = safe_directory('runs')
    removable = []
    removed_files = errors = 0
    for row in candidates:
        # 文件名来自记录也不能信任；只处理固定安全目录下的普通随机日志。
        if not isinstance(row['run_key'], str) or not re.fullmatch(r'[0-9a-f]{32}', row['run_key']):
            errors += 1
            continue
        path = directory / (row['run_key'] + '.log')
        try:
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                # 跳过/过期任务可能从未创建文件，原30天清理也可能已移除正文。
                removable.append(row['id'])
                continue
            if not stat.S_ISREG(mode):
                errors += 1
                continue
            path.unlink()
            removed_files += 1
            removable.append(row['id'])
        except OSError:
            # 不输出路径/原始错误；单个日志失败保留记录，其他记录仍可处理。
            errors += 1
    removed_records = repo.delete_expired_runs(removable, cutoff)
    return {'records': removed_records, 'files': removed_files, 'errors': errors,
            'more': len(candidates) == 200 and bool(removed_records)}

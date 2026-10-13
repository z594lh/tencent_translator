"""单服务器Linux共享锁；只创建固定目录和白名单锁文件。"""
import hashlib
import os
from pathlib import Path

from services.cron_tasks.catalog import GROUPS, ROOT, supported

STATE_ROOT = ROOT / 'log' / 'cron-control'


class BusyError(Exception):
    pass


def identity():
    if not supported():
        return '', -1
    machine = Path('/etc/machine-id').read_text().strip()
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    return hashlib.sha256(f'{machine}:{boot}:{ROOT}'.encode()).hexdigest(), os.getuid()


def host_identity():
    """稳定主机身份用于拒绝第二台服务器接管；重启不改变此值。"""
    if not supported():
        return ''
    machine = Path('/etc/machine-id').read_text().strip()
    return hashlib.sha256(f'{machine}:{ROOT}'.encode()).hexdigest()


def safe_directory(name):
    directory = STATE_ROOT / name
    for path in (ROOT / 'log', STATE_ROOT, directory):
        if path.is_symlink():
            raise RuntimeError('任务目录不允许使用符号链接')
        path.mkdir(mode=0o700, exist_ok=True)
    return directory


class TaskLock:
    """子进程继承fd，即使父执行器异常退出也不能提前释放实际任务的锁。"""
    def __init__(self, group):
        if group not in GROUPS and group != '_worker':
            raise ValueError('未知任务锁')
        self.group = group
        self.fd = None

    def __enter__(self):
        if not supported():
            raise RuntimeError('当前环境不支持任务执行')
        import fcntl
        path = safe_directory('locks') / f'{self.group}.lock'
        self.fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.fd)
            self.fd = None
            raise BusyError('相关任务正在执行，请稍后再试') from None
        except Exception:
            os.close(self.fd)
            self.fd = None
            raise
        return self

    def __exit__(self, *_):
        # 只关闭本进程fd，不主动LOCK_UN，避免解除仍在子进程持有的同一锁。
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def is_busy(group):
    try:
        with TaskLock(group):
            return False
    except BusyError:
        return True

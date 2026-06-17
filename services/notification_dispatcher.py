"""
轻量级事件分发器
进程内同步调用，零外部依赖，零异步开销
适用于低资源服务器，性能等同于直接函数调用
"""
from typing import Callable

_listeners: dict[str, list[Callable]] = {}


def on(event: str):
    """装饰器：注册事件监听器

    @on('order_new')
    def handle_order_new(**kwargs): ...
    """
    def decorator(func: Callable):
        _listeners.setdefault(event, []).append(func)
        return func
    return decorator


def fire(event: str, **kwargs):
    """触发事件，同步调用所有监听器

    每个监听器的异常被独立捕获，不会影响其他监听器或调用方。
    """
    handlers = _listeners.get(event, [])
    if not handlers:
        return
    for handler in handlers:
        try:
            handler(**kwargs)
        except Exception as e:
            print(f"[NotificationDispatcher] 事件 {event} 处理器 {handler.__name__} 异常: {e}")


def listener_count(event: str = None) -> int:
    """返回监听器数量，用于调试"""
    if event:
        return len(_listeners.get(event, []))
    return sum(len(v) for v in _listeners.values())

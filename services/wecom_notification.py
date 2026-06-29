import os

import requests
from dotenv import load_dotenv

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_project_root, ".env"), override=True)

WEBHOOK_URL = os.getenv("WECOM_WEBHOOK_URL", "")


def send_text(content: str, mentioned_list: list = None, mentioned_mobile_list: list = None) -> bool:
    """
    发送文本消息到企业微信群。

    :param content: 文本内容，最长2048字节
    :param mentioned_list: 要@的成员userid列表，@all表示所有人
    :param mentioned_mobile_list: 要@的成员手机号列表
    :return: 是否发送成功
    """
    if not WEBHOOK_URL:
        print("WeCom webhook URL 未配置，跳过通知")
        return False

    body = {
        "msgtype": "text",
        "text": {
            "content": content
        }
    }
    if mentioned_list:
        body["text"]["mentioned_list"] = mentioned_list
    if mentioned_mobile_list:
        body["text"]["mentioned_mobile_list"] = mentioned_mobile_list

    try:
        resp = requests.post(WEBHOOK_URL, json=body, timeout=10)
        result = resp.json()
        if result.get("errcode") == 0:
            return True
        else:
            print(f"企业微信通知发送失败: {result}")
            return False
    except Exception as e:
        print(f"企业微信通知异常: {e}")
        return False


def send_markdown(content: str) -> bool:
    """
    发送Markdown消息到企业微信群。

    :param content: Markdown格式文本
    :return: 是否发送成功
    """
    if not WEBHOOK_URL:
        print("WeCom webhook URL 未配置，跳过通知")
        return False

    body = {
        "msgtype": "markdown",
        "markdown": {
            "content": content
        }
    }

    try:
        resp = requests.post(WEBHOOK_URL, json=body, timeout=10)
        result = resp.json()
        if result.get("errcode") == 0:
            return True
        else:
            print(f"企业微信通知发送失败: {result}")
            return False
    except Exception as e:
        print(f"企业微信通知异常: {e}")
        return False

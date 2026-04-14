import requests
from config import config

# 获取Jellyfin配置
jellyfin_config = config.get_jellyfin_config()
JELLYFIN_URL = jellyfin_config['url']
ADMIN_USERNAME = jellyfin_config['username']
ADMIN_PASSWORD = jellyfin_config['password']

def get_access_token():
    auth_url = f"{JELLYFIN_URL}/Users/AuthenticateByName"
    payload = {
        "Username": ADMIN_USERNAME,
        "Pw": ADMIN_PASSWORD
    }
    headers = {
        "Content-Type": "application/json",
        "X-Emby-Authorization": 'MediaBrowser Client="PythonScript", Device="PC", DeviceId="123456", Version="10.8.0"'
    }
    response = requests.post(auth_url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.json()['AccessToken']
    else:
        raise Exception(f"认证失败，请检查用户名或密码，返回信息: {response.text}")
    

def get_all_items(base_url: str, token: str, limit: int = 1000):
    """
    获取当前用户可见的全部媒体条目（递归所有文件夹）。
    
    :param base_url: Jellyfin 服务根地址，如 http://127.0.0.1:8096
    :param token: 通过 /Users/AuthenticateByName 得到的 AccessToken
    :param limit: 单次请求条数，默认 1000，最大 10000
    :return: 所有 item 的列表，每一项是原始 JSON 对象
    """
    headers = {"X-Emby-Token": token}

    # 1. 先拿到当前用户 ID
    whoami_resp = requests.get(f"{base_url}/Users/Me", headers=headers)
    whoami_resp.raise_for_status()
    user_id = whoami_resp.json()["Id"]

    # 2. 分页拉取
    items_url = f"{base_url}/Users/{user_id}/Items"
    params = {
        "Recursive": "true",
        "IncludeItemTypes": None,
        "Limit": limit,
        "StartIndex": 0
    }

    all_items = []
    start_index = 0
    while True:
        params["StartIndex"] = start_index
        resp = requests.get(items_url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("Items", [])
        if not items:
            break
        all_items.extend(items)
        start_index += len(items)

    return all_items


def get_video_stream_url(item_id, token, video_codec=None, audio_codec=None, container=None):
    """
    根据Jellyfin官方API规范，生成视频直链。
    :param item_id: 视频的Id
    :param token: 认证token
    :param video_codec: 可选，视频编码，如 'h264'、'hevc'
    :param audio_codec: 可选，音频编码，如 'aac'
    :param container: 可选，容器格式，如 'mp4'
    :return: 可直接访问的流媒体URL
    """
    base_url = JELLYFIN_URL.rstrip('/')
    url = f"{base_url}/Videos/{item_id}/stream"
    params = {
        "static": "true",
        "api_key": token
    }
    if video_codec:
        params["videoCodec"] = video_codec
    if audio_codec:
        params["audioCodec"] = audio_codec
    if container:
        params["container"] = container
    # 拼接参数
    from urllib.parse import urlencode
    return url + "?" + urlencode(params)


def get_stream_url(item_id):
    """
    获取视频流的URL，使用Jellyfin的API。
    :return: 视频流的URL
    """
    item_id = str(item_id)  # 确保 item_id 是字符串类型
    if not item_id:
        raise ValueError("item_id 不能为空")
    
    token = get_access_token()
    stream_url = get_video_stream_url(item_id, token, video_codec="hevc", audio_codec="aac")
    
    if not stream_url:
        raise Exception("无法获取视频流URL，请检查Jellyfin服务是否正常运行或配置是否正确。")
    # 返回可直接访问的流媒体URL
    print(f"获取到的视频流URL: {stream_url}")
    return stream_url


if __name__ == "__main__":
    token = get_access_token()
    print("✅ 成功获取访问 Token："+token)

    items = get_all_items(JELLYFIN_URL, token)
    print(f"共拿到 {len(items)} 条 item")
    # 打印前 5 条示例
    for it in items[:5]:
        item_id = it.get("Id")
        stream_url = get_video_stream_url(item_id, token, video_codec="hevc", audio_codec="aac")
        print(it.get("Name"), item_id, stream_url)
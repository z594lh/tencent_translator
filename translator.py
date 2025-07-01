import json
from bs4 import BeautifulSoup, NavigableString
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.tmt.v20180321 import tmt_client, models
import time
import configparser
import os


# 获取当前脚本所在目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")

def read_config():
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"找不到配置文件: {CONFIG_PATH}")

    config.read(CONFIG_PATH, encoding="utf-8")
    
    secret_id = config.get("TencentCloud", "secret_id")
    secret_key = config.get("TencentCloud", "secret_key")
    
    return secret_id, secret_key


# 初始化腾讯云翻译客户端
def init_tencent_client(secret_id, secret_key, region="ap-beijing", endpoint="tmt.tencentcloudapi.com"):
    cred = credential.Credential(secret_id, secret_key, "Token")
    httpProfile = HttpProfile()
    httpProfile.endpoint = endpoint
    clientProfile = ClientProfile()
    clientProfile.httpProfile = httpProfile
    client = tmt_client.TmtClient(cred, region, clientProfile)
    return client

# 单句翻译函数（腾讯云 API）
def translate(text, from_lang='auto', to_lang='zh', client=None):
    if not text or not client:
        return text

    try:
        req = models.TextTranslateRequest()
        params = {
            "SourceText": text,
            "Source": from_lang,
            "Target": to_lang,
            "ProjectId": 0
        }
        req.from_json_string(json.dumps(params))
        resp = client.TextTranslate(req)
        time.sleep(0.2)  # 控制频率
        return resp.TargetText
    except Exception as e:
        print("翻译失败：", e)
        return text  # 返回原文本作为 fallback

# 新增函数：逐句翻译 HTML 内容并还原结构
def translate_html_with_structure(html, client=None, source='en', target='ms'):
    if not html:
        return html

    actual_client = client or tencent_client
    if not actual_client:
        return html
    
    soup = BeautifulSoup(html, 'html.parser')

    for tag in soup.find_all(text=True):  # 找到所有文本节点
        if tag.strip() and tag.parent.name not in ['script', 'style']:  # 忽略脚本和样式
            translated_text = translate(tag.strip(), from_lang=source, to_lang=target, client=actual_client)
            if translated_text:
                tag.replace_with(NavigableString(translated_text))  # 替换为翻译结果

    return str(soup)


# 在模块加载时就初始化好 client
try:
    secret_id, secret_key = read_config()
    tencent_client = init_tencent_client(secret_id, secret_key)
except Exception as e:
    print("【错误】无法初始化腾讯云客户端:", e)
    tencent_client = None



# ================== 示例调用 ==================
if __name__ == "__main__":
    rst = translate('good', from_lang='auto', to_lang='zh', client=tencent_client);
    print(rst)
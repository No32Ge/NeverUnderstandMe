import time
import requests


class APIClient:
    """封装对本地 AI Gateway 的 HTTP 调用"""

    def __init__(self, api_config):
        self.api_key = api_config['api_key']
        self.base_url = api_config['base_url']
        self.model = api_config['model_name']
        self.max_retries = api_config['max_retries']
        self.delay = api_config['delay_between']
        self.max_chars = api_config['max_chars']
        self.max_workers = api_config['max_workers']

        # 🎯 新增：指向本地 FastAPI 网关
        self.gateway_url = api_config.get('gateway_url', "http://127.0.0.1:8000/api/ask")

    def get_response(self, messages):
        """发送消息给本地微服务，失败重试，并返回解析后的 JSON 字典"""

        # 构造发给 FastAPI 的数据包
        payload = {
            "api_key": self.api_key,
            "base_url": self.base_url,
            "model": self.model,
            "messages": messages
        }

        for attempt in range(self.max_retries):
            try:
                # 🎯 这里不再用 client.chat.create，而是直接发 POST 请求给程序 A
                response = requests.post(self.gateway_url, json=payload, timeout=120)

                # 如果 FastAPI 报错（比如解析 JSON 失败、网络超时），抛出异常进入重试逻辑
                response.raise_for_status()

                # FastAPI 返回的已经是干净的 JSON 字典了，直接 return
                return response.json()

            except requests.exceptions.HTTPError as e:
                # 捕获 FastAPI 主动抛出的 422 错误 (JSON解析失败)
                if response.status_code == 422:
                    print(f"   ⚠️ AI 返回不是标准 JSON，重试 ({attempt + 1}/{self.max_retries})")
                else:
                    print(f"   ⚠️ 接口请求错误，重试 ({attempt + 1}/{self.max_retries}): {e}")
                time.sleep(2)
            except Exception as e:
                print(f"   ⚠️ 网关连接失败，重试 ({attempt + 1}/{self.max_retries}): {e}")
                time.sleep(2)

        return None
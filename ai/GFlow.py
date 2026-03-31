import os
import time
import json
import logging
import threading
import queue
import requests  # 🔧 修复：补充了 requests 库导入
from typing import List, Dict, Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import wraps

# 可选依赖，降级处理
try:
    import jinja2
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# 配置日志
logger = logging.getLogger(__name__)


@dataclass
class EngineConfig:
    """统一配置管理"""
    # API 配置
    api_type: str = "openai"          # "openai", "gateway"
    api_key: Optional[str] = None     # 若不提供则从环境变量 OPENAI_API_KEY 读取
    base_url: Optional[str] = None    # 可自定义 API 地址（如 Azure）
    gateway_url: str = "http://127.0.0.1:8000/api/ask"
    model: str = "gpt-3.5-turbo"
    max_retries: int = 3
    request_timeout: int = 120
    max_workers: int = 5

    # 长度限制
    max_prompt_tokens: int = 4000      # 优先使用 token 限制
    max_chars_per_var: int = 4000      # 降级字符限制

    # 限流
    rate_limit_per_worker: float = 0.0  # 每个 worker 最小请求间隔（秒），0 表示不限制

    # 日志
    log_dir: Optional[str] = None
    log_sensitive: bool = False         # 是否记录完整 messages（含可能敏感内容）


class SafeFileLogger:
    """线程安全的异步日志写入器，避免并发文件冲突"""
    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._running = True
        self._thread.start()

    def _worker(self):
        while self._running:
            try:
                item = self._queue.get(timeout=1)
                if item is None:
                    break
                filepath, data = item
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except queue.Empty:
                continue
            except Exception as e:
                logger.warning(f"日志写入失败: {e}")

    def log(self, item_id: Union[int, str], messages: List[Dict], response: Any):
        timestamp = int(time.time() * 1000)  # 毫秒级避免冲突
        filename = f"log_{item_id}_{timestamp}.json"
        filepath = os.path.join(self.log_dir, filename)
        log_data = {
            "timestamp": timestamp,
            "item_id": item_id,
            "full_messages": messages,
            "ai_response": response
        }
        self._queue.put((filepath, log_data))

    def shutdown(self):
        self._running = False
        self._queue.put(None)
        self._thread.join(timeout=2)


class AIFlowEngine:
    """
    AI 自动化处理引擎（生产级增强版）
    支持 OpenAI 原生 API 和自定义网关，内置 Jinja2 模板、Token 限制、线程安全日志等。
    """

    def __init__(
        self,
        config: Union[Dict[str, Any], EngineConfig],
        default_system: str = "You are a helpful assistant. Output purely in JSON.",
        default_template: str = "{{ p1 }}",
        default_few_shots: Optional[List[Dict[str, str]]] = None,
    ):
        if isinstance(config, dict):
            self.config = EngineConfig(**config)
        else:
            self.config = config

        # 自动读取环境变量中的 API Key
        if self.config.api_type == "openai" and not self.config.api_key:
            self.config.api_key = os.getenv("OPENAI_API_KEY", "")
            if not self.config.api_key:
                raise ValueError("OpenAI API key 未提供，请设置 OPENAI_API_KEY 环境变量或传入 config.api_key")

        # 初始化 OpenAI 客户端（若使用原生模式）
        self._openai_client = None
        if self.config.api_type == "openai" and OPENAI_AVAILABLE:
            self._openai_client = openai.OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
                timeout=self.config.request_timeout,
            )

        # 模板引擎
        if JINJA2_AVAILABLE:
            self._jinja_env = jinja2.Environment(
                loader=jinja2.BaseLoader(),
                autoescape=False,
                undefined=jinja2.DebugUndefined  # 保留未定义变量为原样
            )
        else:
            self._jinja_env = None
            logger.warning("Jinja2 未安装，模板将使用简单的字符串替换（不支持条件/循环）")

        # Token 编码器
        self._tokenizer = None
        if TIKTOKEN_AVAILABLE:
            try:
                self._tokenizer = tiktoken.encoding_for_model(self.config.model)
            except KeyError:
                self._tokenizer = tiktoken.get_encoding("cl100k_base")
        else:
            logger.warning("tiktoken 未安装，将使用字符长度近似 token 数（不精确）")

        # 默认参数
        self.default_system = default_system
        self.default_template = default_template
        self.default_few_shots = default_few_shots or []

        # 日志器
        self._logger = SafeFileLogger(self.config.log_dir) if self.config.log_dir else None

        # 限流控制（每个 worker 的请求间隔）
        self._last_request_time = threading.local()

    # 🔧 优化：加入上下文管理器支持
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def _render_template(self, template_str: str, variables: Dict[str, Any]) -> str:
        """渲染模板，优先使用 Jinja2，否则回退到简单替换"""
        if self._jinja_env:
            try:
                tmpl = self._jinja_env.from_string(template_str)
                return tmpl.render(**variables)
            except Exception as e:
                logger.error(f"Jinja2 渲染失败: {e}，回退到简单替换")
                # 失败后继续用简单替换

        # 简单替换（原逻辑增强：支持递归替换）
        result = template_str
        for key, value in variables.items():
            placeholder = f"{{{{ {key} }}}}"  # 支持空格
            result = result.replace(placeholder, str(value))
            placeholder2 = f"{{{{{key}}}}}"   # 无空格
            result = result.replace(placeholder2, str(value))
        return result

    def _truncate_by_tokens(self, text: str, max_tokens: int) -> str:
        """根据 token 数截断文本（保留开头）"""
        if not text:
            return text
        if self._tokenizer:
            tokens = self._tokenizer.encode(text)
            if len(tokens) <= max_tokens:
                return text
            truncated_tokens = tokens[:max_tokens]
            return self._tokenizer.decode(truncated_tokens) + "...(截断)"
        else:
            # 近似：1 token ≈ 4 字符（中英文混合时不准，但作为降级）
            approx_len = max_tokens * 4
            if len(text) <= approx_len:
                return text
            return text[:approx_len] + "...(截断)"

    def _build_messages(
        self,
        prompt: str,
        system_prompt: str,
        few_shots: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """构建消息列表，并校验 few-shot 格式"""
        messages = [{"role": "system", "content": system_prompt}]
        for idx, sample in enumerate(few_shots):
            if "user" not in sample or "assistant" not in sample:
                raise ValueError(f"Few-shot 样本 {idx} 缺少 'user' 或 'assistant' 键")
            messages.append({"role": "user", "content": sample["user"]})
            messages.append({"role": "assistant", "content": sample["assistant"]})
        messages.append({"role": "user", "content": prompt})
        return messages

    def _limit_prompt_tokens(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """限制整个消息列表的总 token 数，优先截断最后一条 user 消息的内容"""
        if not self._tokenizer:
            # 无 tokenizer，使用字符限制近似
            total_chars = sum(len(m["content"]) for m in messages)
            if total_chars <= self.config.max_prompt_tokens * 4:
                return messages
            # 简单截断最后一条 user 消息
            for i in range(len(messages) - 1, -1, -1):
                if messages[i]["role"] == "user":
                    messages[i]["content"] = messages[i]["content"][:self.config.max_prompt_tokens * 4]
                    break
            return messages

        total_tokens = 0
        for m in messages:
            total_tokens += len(self._tokenizer.encode(m["content"]))
        if total_tokens <= self.config.max_prompt_tokens:
            return messages

        # 需要截断：从最后一条 user 消息开始缩减
        excess = total_tokens - self.config.max_prompt_tokens
        for i in range(len(messages) - 1, -1, -1):
            if messages[i]["role"] == "user":
                content = messages[i]["content"]
                content_tokens = self._tokenizer.encode(content)
                if len(content_tokens) > excess:
                    kept = content_tokens[:-excess]
                    messages[i]["content"] = self._tokenizer.decode(kept) + "...(截断)"
                else:
                    messages[i]["content"] = ""  # 极端情况
                break
        return messages

    def _call_openai_api(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """调用 OpenAI 原生 API"""
        if not self._openai_client:
            raise RuntimeError("OpenAI 客户端未初始化，请安装 openai 库或使用 gateway 模式")

        retries = self.config.max_retries
        for attempt in range(retries):
            try:
                response = self._openai_client.chat.completions.create(
                    model=self.config.model,
                    messages=messages,
                    timeout=self.config.request_timeout,
                )
                return {"choices": [{"message": {"content": response.choices[0].message.content}}]}
            except Exception as e:
                logger.warning(f"OpenAI API 调用失败 (尝试 {attempt+1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)  # 指数退避
        raise RuntimeError("OpenAI API 调用彻底失败")

    def _call_gateway_api(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """调用自定义网关 API"""
        payload = {
            "api_key": self.config.api_key,
            "base_url": self.config.base_url,
            "model": self.config.model,
            "messages": messages
        }
        retries = self.config.max_retries
        for attempt in range(retries):
            try:
                resp = requests.post(
                    self.config.gateway_url,
                    json=payload,
                    timeout=self.config.request_timeout
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"网关 API 调用失败 (尝试 {attempt+1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)
        raise RuntimeError("网关 API 调用彻底失败")

    def _request_api(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """统一的 API 请求入口，支持限流"""
        # 限流：每个 worker 独立控制间隔
        if self.config.rate_limit_per_worker > 0:
            now = time.time()
            last = getattr(self._last_request_time, "value", 0)
            sleep_time = self.config.rate_limit_per_worker - (now - last)
            if sleep_time > 0:
                time.sleep(sleep_time)
            self._last_request_time.value = time.time()

        if self.config.api_type == "openai":
            return self._call_openai_api(messages)
        elif self.config.api_type == "gateway":
            return self._call_gateway_api(messages)
        else:
            raise ValueError(f"不支持的 api_type: {self.config.api_type}")

    def ask_single(
        self,
        input_data: Union[str, Dict[str, Any]],
        constants: Optional[Dict[str, Any]] = None,
        template: Optional[str] = None,
        system_prompt: Optional[str] = None,
        few_shots: Optional[List[Dict[str, str]]] = None,
        item_id: Union[int, str] = "single"
    ) -> Optional[Dict[str, Any]]:
        """
        单次 AI 交互
        返回: API 响应字典，若失败则抛出异常
        """
        current_template = template if template is not None else self.default_template
        current_system = system_prompt if system_prompt is not None else self.default_system
        current_few_shots = few_shots if few_shots is not None else self.default_few_shots
        constants = constants or {}

        # 标准化输入
        if isinstance(input_data, str):
            input_map = {"p1": input_data}
        else:
            input_map = dict(input_data)

        # 合并变量（当前输入优先级高于常量）
        merged_vars = {**constants, **input_map}

        # 🔧 修复：必须先对每个变量的值做字符级截断，防止过长，然后再渲染模板
        for key, value in list(merged_vars.items()):
            if isinstance(value, str) and len(value) > self.config.max_chars_per_var:
                merged_vars[key] = value[:self.config.max_chars_per_var] + "\n...(截断)"
            # 非字符串不处理

        # 渲染模板
        rendered_prompt = self._render_template(current_template, merged_vars)

        # 构建消息
        messages = self._build_messages(rendered_prompt, current_system, current_few_shots)

        # Token 限制
        messages = self._limit_prompt_tokens(messages)

        # 请求 API
        try:
            response = self._request_api(messages)
        except Exception as e:
            logger.error(f"ask_single (id={item_id}) 失败: {e}")
            raise

        # 日志记录（仅当配置允许且日志目录存在）
        if self._logger and self.config.log_sensitive:
            self._logger.log(item_id, messages, response)
        elif self._logger:
            # 记录脱敏版本（只保留 response，不记录 messages）
            self._logger.log(item_id, [], response)

        return response

    def process_batch(
        self,
        input_list: List[Union[str, Dict[str, Any]]],
        constants: Optional[Dict[str, Any]] = None,
        template: Optional[str] = None,
        system_prompt: Optional[str] = None,
        few_shots: Optional[List[Dict[str, str]]] = None,
        on_item_complete: Optional[Callable[[int, Any, Optional[Dict[str, Any]]], None]] = None
    ) -> List[Optional[Dict[str, Any]]]:
        """
        并发批处理
        返回结果列表（顺序与原列表一致），失败项为 None
        """
        if not input_list:
            return []

        results = [None] * len(input_list)
        logger.info(f"开始并发处理 {len(input_list)} 条数据，并发数: {self.config.max_workers}")

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_idx = {
                executor.submit(
                    self.ask_single,
                    item,
                    constants,
                    template,
                    system_prompt,
                    few_shots,
                    idx
                ): idx for idx, item in enumerate(input_list)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                res = None  # 初始化结果为 None
                try:
                    res = future.result()
                    results[idx] = res
                    logger.info(f"[{idx}] 完成")
                except Exception as e:
                    results[idx] = None
                    logger.error(f"[{idx}] 失败: {e}")
                finally:
                    # 🔧 优化：无论成功失败都触发回调（失败时 res 为 None，外部可感知并重试/记录 DB）
                    if on_item_complete:
                        try:
                            on_item_complete(idx, input_list[idx], res)
                        except Exception as cb_err:
                            logger.warning(f"回调函数执行失败 (idx={idx}): {cb_err}")

        return results

    def shutdown(self):
        """优雅关闭日志线程"""
        if self._logger:
            self._logger.shutdown()
import os
import json
import threading
import time

file_write_lock = threading.Lock()

class Task:
    """表示一个待处理的项（某个 JSON 文件中的某一行）"""

    def __init__(self, file_stem, item_id, input_map, task_config, append_file, api_client, logger):
        self.file_stem = file_stem
        self.item_id = item_id
        self.input_map = input_map          # 如 {'p1': '...', 'p2': '...'}
        self.task_config = task_config
        self.append_file = append_file
        self.api_client = api_client
        self.logger = logger

    def run(self):
        """执行任务：构造 prompt、调用 API、记录日志、追加结果"""
        # 准备 prompt 文本
        prompt = self.task_config['template']
        for key, content in self.input_map.items():
            placeholder = f"{{{{{key}}}}}"
            if content:
                if len(content) > self.api_client.max_chars:
                    content = content[:self.api_client.max_chars] + "\n...(截断)..."
            else:
                content = "[空内容]"
            prompt = prompt.replace(placeholder, content)

        # 构造消息
        messages = []
        if self.task_config.get('system'):
            messages.append({"role": "system", "content": self.task_config['system']})
        else:
            messages.append({"role": "system", "content": "You are a helpful assistant. Output purely in JSON."})

        # ✅ 使用 samples 列表添加 few-shot 样本
        samples = self.task_config.get('samples', [])
        for sample in samples:
            messages.append({"role": "user", "content": sample['user']})
            messages.append({"role": "assistant", "content": sample['assistant']})

        messages.append({"role": "user", "content": prompt})

        # 调用 API
        time.sleep(self.api_client.delay)
        parsed_ans = self.api_client.get_response(messages)

        # 保存日志
        self.logger.save_interaction(
            self.task_config['name'],
            self.file_stem,
            self.item_id,
            messages,
            parsed_ans
        )

        if parsed_ans is None:
            return f"❌ 失败: {self.file_stem} (ID:{self.item_id})"

        # 线程安全写入 append.json
        try:
            with file_write_lock:
                current = []
                if os.path.exists(self.append_file):
                    try:
                        with open(self.append_file, 'r', encoding='utf-8') as f:
                            current = json.load(f)
                    except:
                        current = []
                current.append({"id": self.item_id, "response": parsed_ans})
                with open(self.append_file, 'w', encoding='utf-8') as f:
                    json.dump(current, f, ensure_ascii=False, indent=2)
            return f"✅ 完成: {self.file_stem} (ID:{self.item_id})"
        except Exception as e:
            return f"❌ 写入失败: {self.file_stem} (ID:{self.item_id}) - {e}"
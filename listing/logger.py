import os
import json
import time

class Logger:
    """记录交互日志"""
    LOG_DIR = "../logs"

    def __init__(self):
        os.makedirs(self.LOG_DIR, exist_ok=True)

    def save_interaction(self, task_name, file_stem, item_id, messages, response_content):
        """保存一次交互的完整日志"""
        timestamp = int(time.time())
        filename = f"log_{task_name}_{file_stem}_id{item_id}_{timestamp}.json"
        filepath = os.path.join(self.LOG_DIR, filename)

        log_data = {
            "timestamp": timestamp,
            "task": task_name,
            "file_stem": file_stem,
            "item_id": item_id,
            "full_messages": messages,
            "ai_response": response_content
        }
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 日志保存失败: {e}")
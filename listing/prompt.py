import os
import re

class PromptManager:
    """管理 Prompt 模板文件，支持 few-shot 样本文件夹，并集成内联任务配置"""
    REQUIRED_FILES = ["template.txt", "system.txt", "example_user.txt", "example_assistant.txt"]

    def __init__(self, prompt_lib_dir, inline_configs=None):
        self.prompt_lib_dir = prompt_lib_dir
        os.makedirs(self.prompt_lib_dir, exist_ok=True)
        self.inline_configs = inline_configs if inline_configs is not None else {}

    def ensure_prompt_exists(self, task_name):
        """检查模板是否存在，不存在则自动创建（仅对文件任务生效）"""
        if task_name in self.inline_configs:
            return True

        prompt_path = os.path.join(self.prompt_lib_dir, task_name)
        os.makedirs(prompt_path, exist_ok=True)

        missing = False
        for fname in self.REQUIRED_FILES:
            fpath = os.path.join(prompt_path, fname)
            if not os.path.exists(fpath):
                missing = True
                default = ""
                if fname == "template.txt":
                    default = "任务模板：\n请根据以下内容回答（请务必输出标准JSON格式）：\n来源1(p1): {{p1}}\n来源2(p2): {{p2}}"
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write(default)
        return not missing

    def load_task_config(self, task_name):
        """加载指定任务的配置（优先返回内联配置，否则从文件读取）"""
        if task_name in self.inline_configs:
            return self.inline_configs[task_name].copy()  # 浅拷贝即可

        folder = os.path.join(self.prompt_lib_dir, task_name)
        if not os.path.isdir(folder):
            return None

        # 读取 template
        try:
            with open(os.path.join(folder, "template.txt"), 'r', encoding='utf-8') as f:
                template = f.read().strip()
        except FileNotFoundError:
            return None
        if not template:
            return None

        # 读取 system（可选）
        system = None
        try:
            with open(os.path.join(folder, "system.txt"), 'r', encoding='utf-8') as f:
                system = f.read().strip()
        except FileNotFoundError:
            pass

        # 扫描 fewshot 文件夹
        pattern = re.compile(r'^fewshot_(\d+)$')
        fewshot_folders = {}
        for item in os.listdir(folder):
            item_path = os.path.join(folder, item)
            if os.path.isdir(item_path):
                m = pattern.match(item)
                if m:
                    num = int(m.group(1))
                    fewshot_folders[num] = item_path

        samples = []

        # 处理 fewshot_0（替换默认 example）
        if 0 in fewshot_folders:
            folder0 = fewshot_folders[0]
            user_file = os.path.join(folder0, "user.txt")
            assistant_file = os.path.join(folder0, "assistant.txt")
            if os.path.exists(user_file) and os.path.exists(assistant_file):
                try:
                    with open(user_file, 'r', encoding='utf-8') as f:
                        user0 = f.read().strip()
                    with open(assistant_file, 'r', encoding='utf-8') as f:
                        assistant0 = f.read().strip()
                    if user0 and assistant0:
                        samples.append({"user": user0, "assistant": assistant0})
                except Exception as e:
                    print(f"⚠️ 警告: 读取 fewshot_0 失败: {e}")
        else:
            # 没有 fewshot_0，则使用默认的 example 文件作为第一个样本
            example_u = None
            example_a = None
            try:
                with open(os.path.join(folder, "example_user.txt"), 'r', encoding='utf-8') as f:
                    example_u = f.read().strip()
            except FileNotFoundError:
                pass
            try:
                with open(os.path.join(folder, "example_assistant.txt"), 'r', encoding='utf-8') as f:
                    example_a = f.read().strip()
            except FileNotFoundError:
                pass
            if example_u and example_a and "undefined" not in example_u:
                samples.append({"user": example_u, "assistant": example_a})

        # 处理其他 fewshot 文件夹（数字 > 0），按数字排序
        other_nums = sorted([num for num in fewshot_folders if num > 0])
        for num in other_nums:
            folder_path = fewshot_folders[num]
            user_file = os.path.join(folder_path, "user.txt")
            assistant_file = os.path.join(folder_path, "assistant.txt")
            if os.path.exists(user_file) and os.path.exists(assistant_file):
                try:
                    with open(user_file, 'r', encoding='utf-8') as f:
                        user_content = f.read().strip()
                    with open(assistant_file, 'r', encoding='utf-8') as f:
                        assistant_content = f.read().strip()
                    if user_content and assistant_content:
                        samples.append({"user": user_content, "assistant": assistant_content})
                except Exception as e:
                    print(f"⚠️ 警告: 读取 {folder_path} 失败: {e}")

        return {
            "name": task_name,
            "template": template,
            "system": system,
            "samples": samples
        }
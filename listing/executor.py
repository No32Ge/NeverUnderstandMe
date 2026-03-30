import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from listing.task import Task
class Executor:
    """调度器：解析任务列表、构建任务队列、多线程执行、后处理"""

    def __init__(self, config, prompt_mgr, data_loader, api_client, logger):
        self.config = config
        self.prompt_mgr = prompt_mgr
        self.data_loader = data_loader
        self.api_client = api_client
        self.logger = logger
        self.jobs = []          # 原始任务组列表 [(dirs, tasks), ...]
        self.queue = []          # 待执行的 Task 对象列表
        self.post_meta = []      # 后处理元数据

    def parse_jobs(self):
        """解析 config_dirs.txt，生成 jobs 列表，并检查模板是否存在"""
        has_template_warning = False
        for line in self.config.job_lines:
            if '|' not in line:
                print(f"⚠️ 行缺少 '|'，已跳过: {line}")
                continue
            src_part, task_part = line.split('|')
            dirs = [self.config.clean_path(d) for d in src_part.split('+') if d.strip()]
            tasks = task_part.split()

            valid_dirs = []
            for d in dirs:
                os.makedirs(d, exist_ok=True)
                valid_dirs.append(d)

            valid_tasks = []
            for t in tasks:
                if self.prompt_mgr.ensure_prompt_exists(t):
                    valid_tasks.append(t)
                else:
                    print(f"🛑 新任务 [{t}] 模板已创建，请修改后重启。")
                    has_template_warning = True

            if valid_dirs and valid_tasks:
                self.jobs.append((valid_dirs, valid_tasks))

        return not has_template_warning   # True 表示可以继续

    def build_queue(self):
        """遍历所有 JSON 文件，构建任务队列，并记录后处理信息"""
        for dir_group, task_group in self.jobs:
            primary_dir = dir_group[0]
            other_dirs = dir_group[1:]

            if not os.path.exists(primary_dir):
                continue
            files = [f for f in os.listdir(primary_dir) if f.endswith('.json')]
            for filename in files:
                primary_path = os.path.join(primary_dir, filename)
                partner_paths = [os.path.join(d, filename) for d in other_dirs]

                primary_data, partner_data_map = self.data_loader.check_consistency(primary_path, partner_paths)
                if primary_data is None:
                    continue

                file_stem, _ = os.path.splitext(filename)
                total = len(primary_data)

                for task_name in task_group:
                    task_cfg = self.prompt_mgr.load_task_config(task_name)
                    if not task_cfg:
                        continue

                    out_dir = os.path.join(primary_dir, task_name)
                    os.makedirs(out_dir, exist_ok=True)
                    append_file = os.path.join(out_dir, f"{file_stem}_append.json")
                    strict_file = os.path.join(out_dir, f"{file_stem}_strict.json")

                    self.post_meta.append({
                        "append_file": append_file,
                        "strict_file": strict_file,
                        "total": total
                    })

                    # 读取已完成 ID
                    completed = set()
                    if os.path.exists(append_file):
                        try:
                            with open(append_file, 'r', encoding='utf-8') as f:
                                existing = json.load(f)
                                for item in existing:
                                    if "id" in item:
                                        completed.add(item["id"])
                        except:
                            pass

                    # 构建未完成的任务
                    for i in range(total):
                        if i in completed:
                            continue
                        input_map = {'p1': primary_data[i]}
                        for key, arr in partner_data_map.items():
                            input_map[key] = arr[i]

                        task = Task(
                            file_stem=file_stem,
                            item_id=i,
                            input_map=input_map,
                            task_config=task_cfg,
                            append_file=append_file,
                            api_client=self.api_client,
                            logger=self.logger
                        )
                        self.queue.append(task)

    def run(self):
        """执行所有任务（多线程）并后处理"""
        if not self.queue:
            print("❌ 没有需要生成的新任务。")
            return

        print(f"\n🚀 开始执行 {len(self.queue)} 个生成任务 (并发: {self.api_client.max_workers})...\n")
        with ThreadPoolExecutor(max_workers=self.api_client.max_workers) as executor:
            futures = {executor.submit(task.run): task for task in self.queue}
            completed = 0
            for future in as_completed(futures):
                res = future.result()
                completed += 1
                print(f"[{completed}/{len(self.queue)}] {res}")

        # 后处理生成 strict.json
        print("\n📦 正在生成严格顺序的最终 JSON 文件...")
        for meta in self.post_meta:
            self._generate_strict(meta['append_file'], meta['strict_file'], meta['total'])

    def _generate_strict(self, append_file, strict_file, total_items):
        """从 append.json 生成排序后且展平的 strict.json"""
        if not os.path.exists(append_file):
            return
        try:
            with open(append_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if len(data) < total_items:
                return   # 未完成，暂不生成
            data.sort(key=lambda x: x.get('id', 0))
            strict_data = []
            for item in data:
                resp = item.get('response')
                if isinstance(resp, list):
                    strict_data.extend(resp)
                else:
                    strict_data.append(resp)
            with open(strict_file, 'w', encoding='utf-8') as f:
                json.dump(strict_data, f, ensure_ascii=False, indent=2)
            print(f"✅ 生成: {os.path.basename(strict_file)}")
        except Exception as e:
            print(f"⚠️ 生成失败 {os.path.basename(strict_file)}: {e}")
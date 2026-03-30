import json
import os
import sys
import yaml
import glob
import re

class Config:
    """加载系统配置文件 system.yaml，其中：
       api_config: 指向 API 配置文件的路径
       dirs_config: 目录列表，每个目录下需包含 .sua 文件，文件内每行格式为 "目录路径 | 任务名"
       prompt_lib_dir: prompt 模板库路径（可选，默认为 "./prompts"）
    """
    SYSTEM_CONFIG_FILE = "system.yaml"

    def __init__(self):
        system_config = self._load_system_config()
        system_dir = os.path.dirname(os.path.abspath(self.SYSTEM_CONFIG_FILE))

        api_config_path = system_config.get("api_config")
        dirs_config = system_config.get("dirs_config")
        prompt_lib_dir = system_config.get("prompt_lib_dir", "./prompts")

        if not api_config_path:
            print("❌ system.yaml 中缺少 'api_config' 字段")
            input("按回车键退出...")
            sys.exit(1)
        if dirs_config is None:
            print("❌ system.yaml 中缺少 'dirs_config' 字段")
            input("按回车键退出...")
            sys.exit(1)
        if not isinstance(dirs_config, list):
            print("❌ system.yaml 中 'dirs_config' 必须是一个列表")
            input("按回车键退出...")
            sys.exit(1)

        self.api_config_path = self._resolve_path(api_config_path, system_dir)
        self.prompt_lib_dir = self._resolve_path(prompt_lib_dir, system_dir)

        self.api_config = self._load_api_config(self.api_config_path)

        # 加载 markers.json，用于内联任务解析
        self.markers = self._load_markers(system_dir)

        # 从 dirs_config 列表中的目录下读取所有 .sua 文件，合并任务行
        self.job_lines, self.inline_task_configs = self._load_sua_lines(dirs_config, system_dir)

    def _load_system_config(self):
        if not os.path.exists(self.SYSTEM_CONFIG_FILE):
            print(f"❌ 找不到系统配置文件 {self.SYSTEM_CONFIG_FILE}")
            input("按回车键退出...")
            sys.exit(1)
        try:
            with open(self.SYSTEM_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(f"❌ system.yaml 格式错误: {e}")
            input("按回车键退出...")
            sys.exit(1)

    def _resolve_path(self, path, base_dir):
        if os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(base_dir, path))

    def _load_api_config(self, path):
        if not os.path.exists(path):
            print(f"❌ 找不到 API 配置文件 {path}")
            input("按回车键退出...")
            sys.exit(1)
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_markers(self, base_dir):
        """加载 markers.json，若文件不存在则返回默认值"""
        default_markers = {
            "start_marker": "[$",
            "end_marker": "$]",
            "separator": "<|||>"
        }
        markers_path = os.path.join(base_dir, "../configs/markers.json")
        if os.path.exists(markers_path):
            try:
                with open(markers_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载 markers.json 失败，使用默认值: {e}")
                return default_markers
        else:
            return default_markers

    def _load_sua_lines(self, dir_list, base_dir):
        """遍历 dir_list 中的每个目录，找到所有 .sua 文件，读取并解析任务行"""
        all_lines = []
        inline_configs = {}

        # 获取起止标记
        start_marker = self.markers.get('start_marker', '[$')
        end_marker = self.markers.get('end_marker', '$]')

        for dir_entry in dir_list:
            dir_path = self._resolve_path(dir_entry, base_dir)
            if not os.path.isdir(dir_path):
                print(f"⚠️ 警告: 目录不存在，已跳过: {dir_path}")
                continue

            sua_files = glob.glob(os.path.join(dir_path, "*.sua"))
            if not sua_files:
                print(f"⚠️ 警告: 目录 {dir_path} 中没有找到 .sua 文件")
                continue

            for sua_file in sua_files:
                try:
                    with open(sua_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()

                    buffer = ""
                    for line in lines:
                        # 对于新的一行，如果 buffer 为空，先忽略纯空行和注释
                        if not buffer:
                            stripped_line = line.strip()
                            if not stripped_line or stripped_line.startswith('#'):
                                continue
                            buffer = line
                        else:
                            buffer += line

                        # 检查 buffer 中 start_marker 和 end_marker 是否成对
                        # 如果起始符号多于结束符号，说明块包含换行尚未闭合，继续拼接下一行
                        if buffer.count(start_marker) > buffer.count(end_marker):
                            continue

                        # 此时 buffer 已经是完整的一个条目（包含跨行的换行符等）
                        stripped = buffer.strip()
                        buffer = ""  # 处理完毕，清空 buffer供下个任务使用

                        if '|' not in stripped:
                            print(f"⚠️ 警告: 行缺少 '|'，已跳过: {stripped[:50]}...")
                            continue

                        # 仅切分第一次出现的 '|'
                        dir_part, task_def_part = stripped.split('|', 1)
                        dir_part = dir_part.strip()
                        task_def_part = task_def_part.strip()

                        # 尝试解析内联任务（即便含多行，这里也会正确匹配正则表达式 \s*）
                        task_name, inline_config = self._parse_inline_task(task_def_part, self.markers)
                        if task_name:
                            # 识别为内联任务
                            if task_name in inline_configs:
                                print(f"⚠️ 警告: 任务名 '{task_name}' 重复定义，后者覆盖前者")
                            inline_configs[task_name] = inline_config
                            # 关键：将含有长篇换行的原行剔除，替换为简洁的 “目录 | 任务短名”
                            new_line = f"{dir_part} | {task_name}"
                            all_lines.append(new_line)
                        else:
                            # 普通任务行，直接添加
                            all_lines.append(stripped)

                    # 容错：如果遍历完文件仍有未闭合的块
                    if buffer:
                        print(f"⚠️ 警告: 文件 {sua_file} 中存在未闭合的 {start_marker}，已被丢弃。")

                except Exception as e:
                    print(f"⚠️ 警告: 读取文件 {sua_file} 失败: {e}")

        return all_lines, inline_configs

    @staticmethod
    def _parse_inline_task(task_def, markers):
        """
        解析内联任务定义字符串。
        返回 (task_name, config_dict) 如果解析成功，否则返回 (None, None)
        """
        start = re.escape(markers['start_marker'])
        end = re.escape(markers['end_marker'])
        sep = markers['separator']

        # 提取任务名
        m = re.match(r'^(\w+)', task_def)
        if not m:
            return None, None
        task_name = m.group(1)
        rest = task_def[m.end():].lstrip()
        # 如果 rest 为空，说明没有任何内联参数块，属于普通文件任务，直接返回
        if not rest:
            return None, None
        config = {
            'name': task_name,
            'system': None,
            'template': None,
            'samples': []
        }
        first_block = True

        while rest:
            # 匹配块: (参数顺序) 空白 start 内容 end 空白
            pattern = r'\(([^)]+)\)\s*' + start + r'(.*?)' + end + r'\s*'
            block_match = re.match(pattern, rest, re.DOTALL)
            if not block_match:
                return None, None
            param_order_str = block_match.group(1).strip()
            content = block_match.group(2)
            rest = rest[block_match.end():].lstrip()

            param_order = param_order_str.split()
            if not all(c in 'suat' for c in param_order):
                print(f"⚠️ 警告: 参数顺序包含非法字符: {param_order_str}")
                return None, None

            values = content.split(sep)
            if len(values) != len(param_order):
                print(f"⚠️ 警告: 参数数量与内容段数不匹配: {param_order_str} vs {len(values)}")
                return None, None

            param_dict = dict(zip(param_order, values))

            if first_block:
                if 't' not in param_dict:
                    print(f"⚠️ 警告: 第一个块缺少 template (t) 参数")
                    return None, None
                config['template'] = param_dict['t']
                config['system'] = param_dict.get('s')
                if 'u' in param_dict and 'a' in param_dict:
                    config['samples'].append({
                        'user': param_dict['u'],
                        'assistant': param_dict['a']
                    })
                first_block = False
            else:
                if 'u' in param_dict and 'a' in param_dict:
                    config['samples'].append({
                        'user': param_dict['u'],
                        'assistant': param_dict['a']
                    })
                if any(c in param_dict for c in ['s', 't']):
                    print(f"⚠️ 警告: 后续块包含 system 或 template，将被忽略")
        return task_name, config

    @staticmethod
    def clean_path(path_str):
        """清洗路径字符串（去除引号、标准化）"""
        return os.path.normpath(path_str.strip().replace('"', '').replace("'", ''))
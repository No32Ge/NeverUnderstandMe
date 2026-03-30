
import os
import json
class DataLoader:
    """加载 JSON 数据文件并校验格式与长度一致性"""

    @staticmethod
    def load_json_array(path):
        """读取并校验必须是 JSON 列表格式"""
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, list):
                print(f"⚠️ [格式不符] {path} 必须是最外层为列表的格式 [...]")
                return None
            return [str(item) for item in data]
        except Exception as e:
            print(f"⚠️ [解析失败] {path} 不是有效的 JSON 文件: {e}")
            return None

    @staticmethod
    def check_consistency(primary_path, partner_paths):
        """检查主文件和副文件是否存在且长度一致"""
        primary_data = DataLoader.load_json_array(primary_path)
        if primary_data is None:
            return None, None

        partner_data_map = {}
        for idx, p_path in enumerate(partner_paths):
            p_key = f"p{idx+2}"
            p_data = DataLoader.load_json_array(p_path)
            if p_data is None:
                print(f"⚠️ 副文件缺失或格式错误: {p_path}")
                return None, None
            if len(p_data) != len(primary_data):
                print(f"⚠️ 长度不匹配: 主文件 {len(primary_data)} 项, 副文件 {len(p_data)} 项")
                return None, None
            partner_data_map[p_key] = p_data

        return primary_data, partner_data_map
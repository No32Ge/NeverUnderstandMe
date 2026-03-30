from config import Config
from prompt import PromptManager
from data import DataLoader
from api import APIClient
from logger import Logger
from executor import Executor

def main():
    print("🔧 初始化 AI 批处理引擎...")
    config = Config()
    prompt_mgr = PromptManager(config.prompt_lib_dir, inline_configs=config.inline_task_configs)
    data_loader = DataLoader()
    api_client = APIClient(config.api_config)
    logger = Logger()

    executor = Executor(config, prompt_mgr, data_loader, api_client, logger)

    if not executor.parse_jobs():
        print("程序已暂停，请修改模板后重启。")
        input("按回车键退出...")
        return

    executor.build_queue()
    executor.run()

    print(f"\n🎉 运行结束！日志已保存在 '{logger.LOG_DIR}' 目录。")

if __name__ == "__main__":
    main()
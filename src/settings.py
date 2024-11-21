# settings.py
__version__ = "0.1.0"
import sys,os
sys.path.append(os.getcwd())
from src.pydantic_settings_file import *
from pathlib import Path
CKG4J_ROOT = Path(os.environ.get("CKG4J_ROOT", ".")).resolve()

class BasicSettings(BaseFileSettings):
    
    model_config = SettingsConfigDict(yaml_file=CKG4J_ROOT / "basic_settings.yaml")
    
    version: str = __version__

    hadoop_streaming_path: str

    hadoop_hdfs_url:str = str("hdfs://0.0.0.0:9000")

    hdfs_user: str = str("root")

    hdfs_url: str = str("")


    @cached_property
    def DATA_PATH(self) -> Path:
        """用户数据根目录"""
        p = CKG4J_ROOT / "data"
        return p

    @cached_property
    def LOG_PATH(self) -> Path:
        """日志存储路径"""
        p = self.DATA_PATH / "logs"
        return p

    def make_dirs(self):
        '''创建所有数据目录'''
        for p in [
            self.DATA_PATH,
            self.LOG_PATH,
        ]:
            p.mkdir(parents=True, exist_ok=True)

class TaskSettings(BaseFileSettings):
    
    model_config = SettingsConfigDict(yaml_file=CKG4J_ROOT / "task_settings.yaml")

    task1: dict = {
        "enable": True,
        "args": {
            "overwrite": True,
        }
    }

    task2: dict = {
        "enable": True,
        "args": {
            "overwrite": True,
        }
    }

    task3: dict = {
        "enable": True,
        "args": {
            "overwrite": True,
        }
    }

    task4: dict = {
        "enable": True,
        "args": {
            "overwrite": True,
            "dumping_factor": 0.85,
            "epsilon": 1e-6,
            "max_iterations": 100,
        }   
    }

    task5: dict = {
        "enable": True,
        "args": {
            "overwrite": True,
            "alpha": 0.85,
        },
    }

    task6: dict = {
        "enable": True,
        "graph_source": 5,
        "args":{
            "top_k_characters": 5,
            "max_iterations": 50,
            "main_character_source": "page_rank",
            "save_temp": False,
            "overwrite": False,
        }
        
    }

class SettingsContainer:
    CKG4J_ROOT = CKG4J_ROOT
    basic_settings: BasicSettings = settings_property(BasicSettings())
    task_settings: TaskSettings = settings_property(TaskSettings())

    def createl_all_templates(self):
        self.basic_settings.create_template_file(write_file=True)
        self.task_settings.create_template_file(write_file=True)

    def set_auto_reload(self, flag: bool=True):
        self.basic_settings.auto_reload = flag
        self.task_settings.auto_reload = flag
    
Settings = SettingsContainer()

if __name__ == "__main__":
    print(Settings.basic_settings.hdfs_url)
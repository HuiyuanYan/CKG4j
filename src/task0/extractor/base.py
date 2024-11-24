from abc import ABC, abstractmethod


class EntityExtractor(ABC):
    """
    抽象基类，定义实体抽取方法
    """
    def __init__(self, entity_type="PERSON"):
        self.entity_type = entity_type

    @abstractmethod
    def extract_entities(self, text):
        """
        抽象方法，用于提取文本中的指定实体
        """
        pass

    def set_entity_type(self, entity_type):
        """
        设置要提取的实体类型
        """
        self.entity_type = entity_type
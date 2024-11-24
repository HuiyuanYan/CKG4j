from .base import EntityExtractor
from .jieba_entity_extractor import JiebaEntityExtractor
from .spacy_entity_extractor import SpacyEntityExtractor
from .nltk_entity_extractor import NltkEntityExtractor

def get_entity_extractor(method, **kwargs) -> EntityExtractor:
    """
    根据指定方法构造实体抽取器。
    
    参数:
        method (str): 实体抽取方法，支持 "spacy", "nltk", "jieba"。
        kwargs: 额外参数，例如 entity_type、model_name 等。
    
    返回:
        EntityExtractor 子类实例
    """
    if method.lower() == "spacy":
        model_name = kwargs.get("model_name", "zh_core_web_sm")
        entity_type = kwargs.get("entity_type", "PERSON")
        return SpacyEntityExtractor(model_name=model_name, entity_type=entity_type)
    elif method.lower() == "nltk":
        entity_type = kwargs.get("entity_type", "PERSON")
        return NltkEntityExtractor(entity_type=entity_type)
    elif method.lower() == "jieba":
        entity_type = kwargs.get("entity_type", "nr")
        return JiebaEntityExtractor(entity_type=entity_type)
    else:
        raise ValueError(f"Unsupported extraction method: {method}")

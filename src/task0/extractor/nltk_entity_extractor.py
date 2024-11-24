import nltk
from nltk import word_tokenize, pos_tag, ne_chunk
from .base import EntityExtractor


class NltkEntityExtractor(EntityExtractor):
    """
    使用 nltk 实现的实体抽取
    """
    def __init__(self, entity_type="PERSON"):
        super().__init__(entity_type)

    def extract_entities(self, text):
        """
        使用 NLTK 提取实体
        """
        entities = []
        tokens = word_tokenize(text)
        tags = pos_tag(tokens)
        tree = ne_chunk(tags)
        for subtree in tree:
            if isinstance(subtree, nltk.Tree) and subtree.label() == self.entity_type:
                entity = " ".join(word for word, tag in subtree)
                entities.append(entity)
        return entities
from sigmai.suzi.inference import Inferer
from config import MODEL_PATH, VOCAB_PATH

inferer = Inferer(MODEL_PATH, VOCAB_PATH)


def tag_articles(articles):
    return inferer.tag_articles(articles, multi_processing=True, batch_size=10000)


def score_articles(articles):
    return inferer.score_articles(articles, multi_processing=True, batch_size=10000)

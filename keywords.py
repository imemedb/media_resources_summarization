from typing import List, Tuple, Union

from gensim.summarization.keywords import keywords
from sklearn.base import BaseEstimator, ClassifierMixin


class KeywordsExtractor(BaseEstimator, ClassifierMixin):
    def fit(self, X, y=None):
        return self

    def predict(self, X, y=None):
        raise NotImplementedError()


class TextrankKeywordsExtractor(KeywordsExtractor):
    def __init__(
        self,
        ratio=0.2,
        words=None,
        split=True,
        scores=False,
        pos_filter=("NN", "JJ"),
        lemmatize=False,
        deacc=True,
    ):
        self.deacc = deacc
        self.lemmatize = lemmatize
        self.pos_filter = pos_filter
        self.scores = scores
        self.split = split
        self.words = words
        self.ratio = ratio

    def predict(self, X: str, y=None) -> Union[List[Tuple[str, float]], List[str], str]:
        return keywords(
            X,
            ratio=self.ratio,
            words=self.words,
            split=self.split,
            scores=self.scores,
            pos_filter=self.pos_filter,
            lemmatize=self.lemmatize,
            deacc=self.deacc,
        )

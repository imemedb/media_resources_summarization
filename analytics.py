from collections import defaultdict, Counter
from logging import Logger
from typing import Dict, List, Any, Tuple, Iterable

import numpy as np

from database import Database


class MediaCounter:
    def __init__(self, db: Database, logger: Logger):
        self.logger = logger
        self.db = db

    @staticmethod
    def __get_biggest_image(list_sizes: List[Dict[Any, Any]]) -> str:
        bigest_size = 0
        url = ""
        for sz in list_sizes:
            if sz["width"] + sz["height"] > bigest_size:
                url = sz["url"]

        return url

    def get_posts_reposts(self, group_name: str):
        raise NotImplementedError

    def get_attachments(self, post) -> List[Tuple[str, str]]:
        """get attachment from post"""
        attachs = []
        if "attachments" in post:
            url = ""
            for a in post["attachments"]:

                if a["type"] == "photo":
                    url = self.__get_biggest_image(a["photo"]["sizes"])
                elif a["type"] == "link":
                    url = a["link"]["url"]

                if not url:
                    self.logger.debug(f'Unknown type of attachment {a["type"]}')

                attachs.append((a["type"], url))
        else:
            self.logger.debug(f'Post {post["id"]} does not have attachments.')
        return attachs

    def get_attachments_per_post(self, group_name: str) -> Dict[str, List[Tuple[str, str]]]:
        """get attachments from group"""
        attachments: Dict[str, List[Tuple[str, str]]] = defaultdict(dict)
        for item in self.db.iter_all_posts(group_name):
            if item["id"] not in attachments:
                attachs = self.get_attachments(item)
                if attachs:
                    attachments[item["id"]] = attachs
        return attachments

    def get_texts_per_post(self, group_name: str) -> Dict[str, str]:
        texts: Dict[str, str] = dict()
        for item in self.db.iter_all_posts(group_name):
            if item["id"] not in texts:
                text = item["text"]
                if text:
                    texts[item["id"]] = text
        return texts

    @staticmethod
    def get_tokens_per_post(texts: Dict[str, str], tokenizer=None) -> Dict[str, List[str]]:
        token_dict = dict()
        for k, t in texts.items():
            token_dict[k] = t.split() if not tokenizer else tokenizer(t)

        return token_dict

    @staticmethod
    def count_tokens(tokens: Dict[str, List[str]]) -> Dict[str, int]:
        return {k: len(v) for k, v in tokens.items()}

    @staticmethod
    def summary(data: Iterable):
        return {
            "mean": float(np.mean(data)),
            "var": float(np.var(data)),
            "max": float(np.max(data)),
            "min": float(np.min(data)),
            "sum": float(np.sum(data)),
        }

    def count(self, group_name: str):
        texts = self.get_texts_per_post(group_name)
        word_counts = self.count_tokens(self.get_tokens_per_post(texts))
        word_stats = self.summary([v for v in word_counts.values()])
        sentence_counts = self.count_tokens(
            self.get_tokens_per_post(texts, tokenizer=lambda t: t.split("."))
        )
        sentence_stats = self.summary([v for v in sentence_counts.values()])

        attachments = self.get_attachments_per_post(group_name)
        attachments_counts = Counter([t[0] for v in attachments.values() for t in v])

        return {
            "word_stats": word_stats,
            "sentence_stats": sentence_stats,
            "attachments": attachments_counts,
        }

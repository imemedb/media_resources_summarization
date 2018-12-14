import json
import os
from collections import defaultdict, Counter
from typing import List, Dict, Any, Iterator, Tuple, Iterable
import logging

import gensim
import pandas as pd
import numpy as np
from gensim import corpora
from gensim.models import LdaModel

logger = logging.getLogger('database')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('database.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

sh = logging.StreamHandler()
sh.setLevel(logging.ERROR)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)


class Database:

    def __init__(self, source):
        self.source = source

    @property
    def n_resources(self) -> int:
        return len(os.listdir(self.source))

    @property
    def group_names(self) -> List[str]:
        return os.listdir(self.source)

    @property
    def groups_paths(self) -> List[str]:
        return [os.path.join(self.source, name) for name in os.listdir(self.source)]

    def group_path(self, group_name: str) -> str:
        return os.path.join(self.source, group_name)

    def count_posts(self, group_name=None) -> Dict[str, int]:
        counts = dict()
        if group_name:
            try:
                counts[group_name] = len(os.listdir(self.group_path(group_name)))
            except FileNotFoundError as e:
                print(e)
        else:
            for gp in self.groups_paths:
                counts[gp.split('/')[-1]] = len(os.listdir(gp))

        return counts

    def head(self, group_name: str, n: int = 5) -> List[Dict[Any, Any]]:
        path = self.group_path(group_name)
        posts_paths = [os.path.join(path, p, 'info.json') for p in os.listdir(path)[:n]]
        posts = []
        for p in posts_paths:
            with open(p, 'r') as f:
                posts.append(json.load(f))

        return posts

    def iter_all_posts(self, group_name: str) -> Iterator[Dict[Any, Any]]:
        path = self.group_path(group_name)

        for pp in os.listdir(path):
            with open(os.path.join(path, pp, 'info.json'), 'r') as f:
                yield json.load(f)

    @staticmethod
    def __get_biggest_image(list_sizes: List[Dict[Any, Any]]) -> str:
        bigest_size = 0
        url = ''
        for sz in list_sizes:
            if sz['width'] + sz['height'] > bigest_size:
                url = sz['url']

        return url

    def get_posts_reposts(self, group_name: str):
        raise NotImplementedError

    def get_attachments(self, post) -> List[Tuple[str, str]]:
        attachs = []
        try:
            url = ''
            for a in post['attachments']:

                if a['type'] == 'photo':
                    url = self.__get_biggest_image(a['photo']['sizes'])
                elif a['type'] == 'link':
                    url = a['link']['url']

                if not url:
                    logger.debug(f'Unknown type of attachment {a["type"]}')

                attachs.append((a['type'], url))
        except KeyError as e:
            logger.debug(f'Post {post["id"]} does not have attachments.')
        return attachs

    def get_attachments_per_post(self, group_name: str) -> Dict[str, List[Tuple[str, str]]]:
        attachments: Dict[str, List[Tuple[str, str]]] = defaultdict(dict)
        for batch in self.iter_all_posts(group_name):
            for item in batch['items']:
                if item['id'] not in attachments:
                    attachs = self.get_attachments(item)
                    if attachs:
                        attachments[item['id']] = attachs
        return attachments

    def get_texts_per_post(self, group_name: str) -> Dict[str, str]:
        texts: Dict[str, str] = dict()
        for batch in self.iter_all_posts(group_name):
            for item in batch['items']:
                if item['id'] not in texts:
                    text = item['text']
                    if text:
                        texts[item['id']] = text
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
            'mean': np.mean(data),
            'var': np.var(data),
            'max': np.max(data),
            'min': np.min(data),
            'sum': np.sum(data)
        }

    def count_media(self, group_name: str, plot=True):
        texts = self.get_texts_per_post(group_name)
        word_counts = self.count_tokens(self.get_tokens_per_post(texts))
        word_stats = self.summary([v for v in word_counts.values()])
        sentence_counts = self.count_tokens(self.get_tokens_per_post(texts, tokenizer=lambda t: t.split('.')))
        sentence_stats = self.summary([v for v in sentence_counts.values()])

        attachments = self.get_attachments_per_post(group_name)
        attachments_counts = Counter([t[0] for v in attachments.values() for t in v])

        return {
            'word_stats': word_stats,
            'sentence_stats': sentence_stats,
            'attachments': attachments_counts
        }

    def build_language_model(self, group_name, tokenizer=None) -> LdaModel:
        # TODO: add check for existing language model and load it if it's not too old
        texts = self.get_texts_per_post(group_name)
        tokens = self.get_tokens_per_post(texts, tokenizer)
        dictionary = corpora.Dictionary([t for t in tokens.values()])
        corpus = [dictionary.doc2bow(t) for t in tokens.values()]
        lda: LdaModel = LdaModel(corpus=corpus, id2word=dictionary, num_topics=5, update_every=1, chunksize=10000,
                                 passes=1)

        return lda

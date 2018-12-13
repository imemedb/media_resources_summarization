import json
import os
from collections import defaultdict
from typing import List, Dict, Any, Iterator, Tuple
import logging

import pandas as pd


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

    def get_attachments(self, post):
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

    def get_attachments_per_post(self, group_name: str) -> Dict[str, List[str]]:
        attachments: Dict[str, List[str]] = defaultdict(dict)
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

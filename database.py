import json
import logging
import os
from typing import List, Dict, Any, Iterator, Union

from pymongo import MongoClient

logger = logging.getLogger("database")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

fh = logging.FileHandler("database.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

sh = logging.StreamHandler()
sh.setLevel(logging.ERROR)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)


class Database:
    def __init__(self, source, save_path):
        self.save_path = save_path
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
                counts[gp.split("/")[-1]] = len(os.listdir(gp))

        return counts

    def head(self, group_name: str, n: int = 5) -> List[Dict[Any, Any]]:
        path = self.group_path(group_name)
        posts_paths = [os.path.join(path, p, "info.json") for p in os.listdir(path)[:n]]
        posts = []
        for p in posts_paths:
            with open(p, "r") as f:
                posts.append(json.load(f))

        return posts

    def iter_all_posts(self, group_name: str) -> Iterator[Dict[Any, Any]]:
        path = self.group_path(group_name)

        for pp in os.listdir(path):
            with open(os.path.join(path, pp, "info.json"), "r") as f:
                for post in json.load(f)["items"]:
                    yield post

    def save_data(self, data: Union[List[dict], dict]):
        """
        Method to save post in a dictionary format
        :param post: dict of data to save
        :param community_name: name of community
        :param post_id: id of post on resource
        :return:
        """
        if isinstance(data, dict):
            community_name = data["group_name"]
            post_id: str = data["_id"]
            community_path = os.path.join(self.save_path, community_name)
            post_path = os.path.join(community_path, str(post_id))
            if not os.path.exists(community_path):
                os.mkdir(community_path)
            if not os.path.exists(post_path):
                os.mkdir(post_path)

            with open(f"{post_path}/info.json", "w") as f:
                json.dump(data, f, indent=4)
        else:
            for d in data:
                self.save_data(d)


class MongoDatabase(Database):
    def __init__(
        self,
        source,
        mongo_client: MongoClient,
        database_name: str,
        collection_name: str,
        *args,
        **kwargs,
    ):
        super().__init__(source, None)
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = mongo_client
        self.db = self.client[self.database_name]
        self.collection = self.db[self.collection_name]

    def save_data(self, data: Union[List[dict], dict]):
        if isinstance(data, dict):
            id_doc: str = data["_id"]
            existing_doc = self.collection.find_one({"_id": id_doc})
            if existing_doc is None:
                self.collection.insert_one(data)
            else:
                self.collection.replace_one({"_id": id_doc}, data)
        else:
            for d in data:
                self.save_data(d)

    @property
    def n_resources(self) -> int:
        return len(self.group_names)

    @property
    def group_names(self) -> List[str]:
        return list({o["group_name"] for o in self.collection.find({}, {"group_name": 1})})

    def count_posts(self, group_name=None) -> Dict[str, int]:
        counts = dict()
        if group_name is not None:
            group_posts = self.collection.find({"group_name": group_name}, {"group_name": 1})
            counts[group_name] = len([*group_posts]) if group_posts else 0
        else:
            for gn in self.group_names:
                counts.update(self.count_posts(gn))

        return counts

    def iter_all_posts(self, group_name: str) -> Iterator[Dict[Any, Any]]:
        for post in self.collection.find({"group_name": group_name}):
            yield post

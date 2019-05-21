import json
import logging
import os
from typing import List, Dict, Optional

import requests
import vk_api
from bs4 import BeautifulSoup

from database import Database

logger = logging.getLogger("scrapers")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

fh = logging.FileHandler("vk_loader_logs.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

sh = logging.StreamHandler()
sh.setLevel(logging.ERROR)
sh.setFormatter(formatter)


logger.addHandler(fh)
logger.addHandler(sh)


class ScraperException(Exception):
    def __init__(self, msg: str = ""):
        self.msg = msg

    def __str__(self):
        return f"Exception occurred while scraping. {self.msg}"


class Scraper:
    def __init__(self, resource_name: str) -> None:
        """
        Base class for scraping resources

        :param save_path: where to save incoming data
        :param resource_name: name of resource from resources.json
        """

        self.resource_name = resource_name

    def process_community(self, name: str):
        raise NotImplementedError("You should implement the method to process communities")

    def add_community_to_resources(self, community_name):
        with open("resources.json", "r") as f:
            res = json.load(f)
        if community_name not in res[self.resource_name]["communities"]:
            res[self.resource_name]["communities"].append(community_name)
            with open("resources.json", "w") as f:
                json.dump(res, f, indent=4)

    def scrap_data(self) -> None:
        """
        Scraps data from communities in `resources.json`

        :return:
        """
        with open("resources.json", "r") as f:
            communities: List[str] = json.load(f)[self.resource_name]["communities"]

        for community in communities:
            self.process_community(community)


class VkScraper(Scraper):
    def __init__(self, email: str, password: str, database: Database, resource_name="vk") -> None:
        """
        Initialize vk scraper

        :param email: Your email or phone to vk
        :param password: Your password to vk
        :param save_path: Where to store data.
                          By default will save in the folder where program was started.
        """
        super().__init__(resource_name=resource_name)
        self.database = database
        self.vk_session = vk_api.vk_api.VkApi(email, password)
        self.vk_session.auth(token_only=True)
        self.vk = self.vk_session.get_api()
        logger.debug(f"{self.resource_name} - Connected to vk API")

    def process_community(self, name: str) -> None:
        """
        Save data from one community

        :param name: so called "domain" name of community
        """
        data: dict = self.vk_session.method("wall.get", {"domain": name, "count": 100})
        data = data["items"]
        for d in data:
            d["group_name"] = name
            d["_id"] = str(d["id"]) + str(d["from_id"])
        self.database.save_data(data)


class TelegramScraper(Scraper):
    def __init__(self, resource_name: str = "telegram", save_path: str = "telegram_data/") -> None:
        """
        Scraper for telegram channels.
        :param save_path: where to save incoming data
        :param resource_name: name of resource from resources.json
        """
        super().__init__(save_path, resource_name)
        self.base_url: str = "https://t.me"

    def get_content(self, soup: BeautifulSoup) -> Optional[str]:
        """
        If there is anything useful of page, will return string, else None
        :param soup:
        :return:
        """
        general_info = soup.find("div", {"class": "tgme_page_description"})
        content: str = soup.find("meta", attrs={"name": "twitter:description"})[
            "content"
        ] if not general_info else None
        return content

    def process_post(self, url: str) -> Optional[Dict[str, str]]:
        """
        Get useful data from page.
        :param url: url to shared post
        :return:
        """
        resp = requests.get(url)
        data = None
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content.decode(), features="lxml")
            content = self.get_content(soup)
            data = {"url": url, "content": content} if content else None
        return data

    def process_community(self, name: str) -> None:
        """
        Get data from community
        :param name: name of community from `resources.json`
        :return:
        """
        save_path = os.path.join(self.save_path, name)
        if os.path.exists(save_path):
            posts = {int(p) for p in os.listdir(save_path) if "." not in p}
            id_ = max(posts)
        else:
            raise ScraperException(
                "You should call `initialize_channel` with giving any post from it to this method."
            )

        logger.debug(f"{self.resource_name} - Id of of the latest saved post is {id_}")
        n_new_post = 0

        while True:
            # Getting new post
            url = f"{self.base_url}/{name}/{id_}"
            post = self.process_post(url)
            # Saving or ignoring it
            if post and id_ not in posts:
                self.save_post(post, name, str(id_))
            else:
                break
            # Updating indexing
            id_ += 1
            n_new_post += 1

        logger.debug(f"{self.resource_name} - There are {n_new_post} new posts in {name}")

    def initialize_channel(self, url: str) -> None:
        """
        If community was not processed before, you should first process any post from there
        :param url: url to post
        """
        data = self.process_post(url)
        community_name, post_id = url.split("/")[-2:]
        if data:
            self.save_post(data, community_name, post_id)
            self.add_community_to_resources(community_name)

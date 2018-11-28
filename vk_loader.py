import json
import logging
import os

import vk_api

logger = logging.getLogger('vk_loader')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('vk_loader_logs.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

sh = logging.StreamHandler()
sh.setLevel(logging.ERROR)
sh.setFormatter(formatter)


logger.addHandler(fh)
logger.addHandler(sh)


class VkScraper:

    def __init__(self, email, password, save_path=None):
        self.vk_session = vk_api.vk_api.VkApi(email, password)
        self.vk_session.auth(token_only=True)
        self.vk = self.vk_session.get_api()
        logging.debug('Connected to vk API')
        self.save_path = save_path or 'vk_data/'
        if not os.path.exists(self.save_path):
            os.mkdir(self.save_path)

    def process_community(self, name):
        save_path = os.path.join(self.save_path, name)
        if not os.path.exists(save_path):
            os.mkdir(save_path)
            logger.debug(f'Created folder for {name} community')
        news_set = set(os.listdir(save_path))
        data = self.vk_session.method('wall.get', {'domain': name})
        new_news = 0
        for item in data['items']:
            if str(item['id']) not in news_set:
                news_path = os.path.join(save_path, str(item['id']))
                os.mkdir(news_path)
                with open(os.path.join(news_path, 'info.json'), 'w') as f:
                    json.dump(item, f, indent=4)
                new_news += 1
        logger.debug(f'{name} has {new_news} new posts')
        return data

    def scrap_data(self):
        with open('resources.json', 'r') as f:
            communities = json.load(f)['vk']['communities']

        for community in communities:
            self.process_community(community)


if __name__ == '__main__':
    from config import *
    scraper = VkScraper(EMAIL, PASSWORD)
    scraper.scrap_data()

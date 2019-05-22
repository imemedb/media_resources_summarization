import logging
from http import HTTPStatus

import pymongo
from flask import Flask, request, jsonify
from pymongo import MongoClient

from analytics import MediaCounter
from config import *
from database import MongoDatabase
from scrapers import VkScraper

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


app = Flask(__name__)
mongo_client = MongoClient()
db = MongoDatabase(
    source="vk", mongo_client=mongo_client, database_name="vk", collection_name="posts"
)
counter = MediaCounter(db, logger=logger)
scrapper = VkScraper(email=EMAIL, password=PASSWORD, database=db)


@app.errorhandler(HTTPStatus.METHOD_NOT_ALLOWED)
def method_not_allowed(e):
    return jsonify({"error": str(e)}), HTTPStatus.METHOD_NOT_ALLOWED


@app.errorhandler(HTTPStatus.INTERNAL_SERVER_ERROR)
def server_error(e):
    return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR


@app.errorhandler(Exception)
def error_happened(e):
    return jsonify({"error": str(e)}), HTTPStatus.BAD_REQUEST


@app.route("/v0/update_group", methods=["POST"])
def update_group():
    assert request.json["group_name"], "group_name is missing"

    group_name = request.json["group_name"]
    scrapper.process_community(group_name)

    return jsonify({})


@app.route("/v0/count_media", methods=["GET"])
def count_media():
    assert request.json["group_name"], "group_name is missing"

    group_name = request.json["group_name"]
    return jsonify(counter.count(group_name))


@app.route("/v0/get_posts", methods=["GET"])
def get_posts():
    assert request.json["group_name"], "group_name is missing"

    group_name = request.json["group_name"]
    # order values: top, recent
    order = request.json.get("order", "top")
    assert order in {"top", "recent"}, "order should have `recent` or `top` value"
    skip = request.json.get("skip", 0)
    limit = request.json.get("limit", 10)

    results = db.collection.find({"group_name": group_name})
    if order == "top":
        results.sort("likes.count", pymongo.DESCENDING)

    results.limit(limit).skip(skip)

    return jsonify(list(results))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5055, debug=True)

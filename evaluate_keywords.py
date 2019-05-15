import argparse
import json
import logging
import os
from pathlib import Path
from zipfile import ZipFile

import nltk
import pke
import pymorphy2
import yake
from keyverbum import keywords
from keyverbum.evaluate import evaluate
from keyverbum.keywords import TfIdf, TopicalPagerank, PymorphyStemmer, BasicPreprocessor
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logging.getLogger(pke.__name__).setLevel(logging.WARNING)


parser = argparse.ArgumentParser()
parser.add_argument("--data_path")
parser.add_argument("--dataset")
parser.add_argument("--algorithm")
args = parser.parse_args()


def get_predictions(text):
    try:
        extractor = pke.unsupervised.YAKE()
        extractor.load_document(input=text, language="ru2")
        extractor.candidate_selection(stoplist=nltk.corpus.stopwords.words("russian"))
        extractor.candidate_weighting()
        keyphrases = extractor.get_n_best(n=10)
        keyphrases
        return keyphrases
    except Exception:
        return []


def get_dataset(name):
    data: List[dict] = []
    for z_name in os.listdir(dataset_path):
        if name in z_name or name == "all":
            with ZipFile(dataset_path / z_name) as z, z.open(z.namelist()[0]) as f:
                for str_obj in f:
                    new_file = json.loads(str_obj.decode())
                    if name == "cyberleninka":
                        new_file["keywords"] = [k.lower() for k in new_file["keywords"]]
                        new_file["content"] = (
                            new_file["abstract"].lower()
                            if new_file["abstract"]
                            else new_file["content"]
                        )
                    data.append(new_file)
    return data


dataset_path = Path(args.data_path)
ng = get_dataset(args.dataset)

swf = keywords.StopwordsFilter("russian")
morph = pymorphy2.MorphAnalyzer()

kw_pred = []
kw_gt = []
for news in tqdm(ng):
    try:
        if args.algorithm == "tfidf":
            extractor = TfIdf(swf, ngram_range=(1, 3))
            extractor.fit([news["content"]])
            pred = [k for k in extractor.predict([news["content"]])]
        elif args.algorithm == "topicrank":
            prep = BasicPreprocessor()
            stemmer = PymorphyStemmer(morph)
            extractor = TopicalPagerank(
                morph=morph, stemmer=stemmer, preprocessor=prep, stopwords=swf, lang="russian"
            )

            extractor.fit(news["content"])
            pred = extractor.predict(10)
        elif args.algorithm == "yake":
            extractor = yake.KeywordExtractor(lan="ru", n=3, dedupLim=0.8, windowsSize=2, top=10)
            pred = [p[0] for p in extractor.extract_keywords(news["content"])]
        elif args.algorithm == "textrank":
            extractor = keywords.Textrank(words=10)
            pred = extractor.predict(news["content"])

        kw_pred.append(pred)
        kw_gt.append([k.lower() for k in news["keywords"]])
    except Exception as e:
        print(e)
evaluate(kw_pred, kw_gt)

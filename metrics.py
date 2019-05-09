from typing import List

import numpy as np
from sklearn.metrics import precision_recall_fscore_support
from sklearn.preprocessing import MultiLabelBinarizer


def multilabel_classification_report(
    keywords_1: List[List[str]], keywords_2: List[List[str]], average="weighted"
):
    ohe = MultiLabelBinarizer()
    ohe.fit(np.array(keywords_1 + keywords_2))
    m1, m2 = ohe.transform(keywords_1), ohe.transform(keywords_2)
    return precision_recall_fscore_support(m1, m2, average=average)

from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
import numpy as np
from scipy.special import softmax
import csv
import urllib.request

from smartpy.utility.log_util import getLogger

logger = getLogger(__name__)

ROBERTA_EMOTION_TASKS = ['emotion', 'sentiment', 'hate', 'irony', 'offensive']


class TwitterRobertaBaseNLP:

    def __init__(self,
                 dir,
                 task):
        self.task = task
        model_name = f'cardiffnlp/twitter-roberta-base-{task}'
        self.tokenizer = AutoTokenizer.from_pretrained(dir+f"/models_data/huggingface/{task}_tokenizer")
        self.dl_model = AutoModelForSequenceClassification.from_pretrained(dir+f"/models_data/huggingface/{task}_model")

        # download label coin_category_mapping
        mapping_link = f"https://raw.githubusercontent.com/cardiffnlp/tweeteval/main/datasets/{task}/mapping.txt"
        with urllib.request.urlopen(mapping_link) as f:
            html = f.read().decode('utf-8').split("\n")
            csvreader = csv.reader(html, delimiter='\t')
        self.labels = [row[1] for row in csvreader if len(row) > 1]

    def predict(self, clean_text: str):
        encoded_input = self.tokenizer(clean_text, return_tensors='pt')
        output = self.dl_model(**encoded_input)
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)
        ranking = np.argsort(scores)
        ranking = ranking[::-1]
        emotions = {}
        for i in range(scores.shape[0]):
            l = self.labels[ranking[i]]
            s = scores[ranking[i]]
            emotions[l] = np.round(float(s), 4)
        return emotions



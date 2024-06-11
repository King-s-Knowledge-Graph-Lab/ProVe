import json
import numpy as np
import pandas as pd
from pathlib import Path
import torch
import re

from transformers import BertTokenizer, BertForSequenceClassification

# Constants and paths
HOME = Path('/users/k2031554')
DEVICE = 'cuda:0' if torch.cuda.is_available() else 'cpu'
MAX_LEN = 1024
CLASSES = ['SUPPORTS','REFUTES','NOT ENOUGH INFO']
METHODS = ['WEIGHTED_SUM', 'MALON']

def process_sent(sentence):
    sentence = re.sub("LSB.*?RSB", "", sentence)
    sentence = re.sub("LRB\s*?RRB", "", sentence)
    sentence = re.sub("(\s*?)LRB((\s*?))", "\\1(\\2", sentence)
    sentence = re.sub("(\s*?)RRB((\s*?))", "\\1)\\2", sentence)
    sentence = re.sub("--", "-", sentence)
    sentence = re.sub("``", '"', sentence)
    sentence = re.sub("''", '"', sentence)    
    return sentence

class TextualEntailmentModule():

    def __init__(
        self,
        model_path = 'base/models/BERT_FEVER_v4_model_PBT',
        tokenizer_path = 'base/models/BERT_FEVER_v4_tok_PBT'
        ):
        self.tokenizer = BertTokenizer.from_pretrained(
            tokenizer_path
        )
        self.model = BertForSequenceClassification.from_pretrained(
            model_path
        )
        self.model.to(DEVICE)

    #def get_pair_scores(self, claim, evidence):
    #    
    #    encodings = self.tokenizer(
    #        [claim, evidence],
    #        max_length= MAX_LEN,
    #        return_token_type_ids=False,
    #        padding='max_length',
    #        truncation=True,
    #        return_tensors='pt',
    #    ).to(DEVICE)
    #
    #    self.model.eval()
    #    with torch.no_grad():
    #        probs = self.model(
    #            input_ids=encodings['input_ids'],
    #            attention_mask=encodings['attention_mask']
    #        )
    #    
    #    return torch.softmax(probs.logits,dim=1).cpu().numpy()

    def get_batch_scores(self, claims, evidence):

        inputs = list(zip(claims, evidence))
        
        encodings = self.tokenizer(
            inputs,
            max_length= MAX_LEN,
            return_token_type_ids=False,
            padding='max_length',
            truncation=True,
            return_tensors='pt',
        ).to(DEVICE)

        self.model.eval()
        with torch.no_grad():
            probs = self.model(
                input_ids=encodings['input_ids'],
                attention_mask=encodings['attention_mask']
            )
        
        return torch.softmax(probs.logits,dim=1).cpu().numpy()

    def get_label_from_scores(self, scores):
        return CLASSES[np.argmax(scores)]

    def get_label_malon(self, score_set):
        score_labels = [np.argmax(s) for s in score_set]
        if 1 not in score_labels and 0 not in score_labels:
            return CLASSES[2] #NOT ENOUGH INFO
        elif 0 in score_labels:
            return CLASSES[0] #SUPPORTS
        elif 1 in score_labels:
            return CLASSES[1] #REFUTES

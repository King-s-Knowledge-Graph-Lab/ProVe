import re
from typing import List, Tuple
import pathlib

import torch
from transformers import BertTokenizer

from utils.sentence_retrieval_model import sentence_retrieval_model


THIS_DIR = pathlib.Path(__file__).parent.absolute()
ARGS = {
    'batch_size': 32,
    'bert_pretrain': 'base/bert_base',
    'checkpoint': 'base/model.best.32.pt',
    'dropout': 0.6,
    'bert_hidden_dim': 768,
    'max_len': 384,
    'cuda': torch.cuda.is_available()
}

if not ARGS['cuda']:
    print('CUDA NOT AVAILABLE')


def process_sent(sentence):
    sentence = re.sub("LSB.*?RSB", "", sentence)
    sentence = re.sub("LRB\s*?RRB", "", sentence)
    sentence = re.sub("(\s*?)LRB((\s*?))", "\\1(\\2", sentence)
    sentence = re.sub("(\s*?)RRB((\s*?))", "\\1)\\2", sentence)
    sentence = re.sub("--", "-", sentence)
    sentence = re.sub("``", '"', sentence)
    sentence = re.sub("''", '"', sentence)    
    return sentence

class SentenceRetrievalModule():

    def __init__(self, max_len=None):
        
        if max_len:
            ARGS['max_len'] = max_len
        
        self.tokenizer = BertTokenizer.from_pretrained(ARGS['bert_pretrain'], do_lower_case=False)
        self.model = sentence_retrieval_model(ARGS)
        self.model.load_state_dict(torch.load(ARGS['checkpoint'], map_location=torch.device('cpu'))['model'])
        if ARGS['cuda']:
            self.model = self.model.cuda()

    def score_sentence_pairs(self, inputs: List[Tuple[str]]):
        inputs_processed = [(process_sent(input[0]), process_sent(input[1])) for input in inputs]

        encodings =  self.tokenizer(
            inputs_processed,
            padding='max_length',
            truncation='longest_first',
            max_length=ARGS['max_len'],
            return_token_type_ids=True,
            return_attention_mask=True,
            return_tensors='pt',
        )

        inp = encodings['input_ids']
        msk = encodings['attention_mask']
        seg = encodings['token_type_ids']

        if ARGS['cuda']:
            inp = inp.cuda()
            msk = msk.cuda()
            seg = seg.cuda()

        self.model.eval()
        with torch.no_grad():
            outputs = self.model(inp, msk, seg).tolist()

        assert len(outputs) == len(inputs)

        return outputs
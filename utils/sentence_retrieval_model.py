import torch
import torch.nn as nn

from utils.bert_model import BertForSequenceEncoder

class sentence_retrieval_model(nn.Module):
    def __init__(self, args):
        super(sentence_retrieval_model, self).__init__()
        self.pred_model = BertForSequenceEncoder.from_pretrained(args['bert_pretrain'])
        self.bert_hidden_dim = args['bert_hidden_dim']
        self.dropout = nn.Dropout(args['dropout'])
        self.proj_match = nn.Linear(self.bert_hidden_dim, 1)


    def forward(self, inp_tensor, msk_tensor, seg_tensor):
        _, inputs = self.pred_model(inp_tensor, msk_tensor, seg_tensor)
        inputs = self.dropout(inputs)
        score = self.proj_match(inputs).squeeze(-1)
        score = torch.tanh(score)
        return score
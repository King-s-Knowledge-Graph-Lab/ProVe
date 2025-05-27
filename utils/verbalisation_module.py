from utils.finetune import Graph2TextModule
from typing import Dict, List, Tuple, Union, Optional
import torch
import re

if torch.cuda.is_available():
    DEVICE = 'cuda'
else:
    DEVICE = 'cpu'
    print('CUDA NOT AVAILABLE')

CHECKPOINT = '/home/ubuntu/RQV/base/t5-base_13881_val_avg_bleu=68.1000-step_count=5.ckpt'
MAX_LENGTH = 384
SEED = 42


class VerbModule():
    
    def __init__(self, override_args: Dict[str, str] = None):
        # Model
        if not override_args:
            override_args = {}
        self.g2t_module = Graph2TextModule.load_from_checkpoint(CHECKPOINT, strict=False, **override_args)
        self.tokenizer = self.g2t_module.tokenizer
        # Unk replacer
        self.vocab = self.tokenizer.get_vocab()
        self.convert_some_japanese_characters = True
        self.unk_char_replace_sliding_window_size = 2
        self.unknowns = []

    def __generate_verbalisations_from_inputs(self, inputs: Union[str, List[str]]):
        try:
            inputs_encoding = self.tokenizer.prepare_seq2seq_batch(
                inputs, truncation=True, max_length=MAX_LENGTH, return_tensors='pt'
            )
            inputs_encoding = {k: v.to(DEVICE) for k, v in inputs_encoding.items()}
            
            self.g2t_module.model.eval()
            with torch.no_grad():
                # Add decoder_start_token_id configuration
                self.g2t_module.model.config.decoder_start_token_id = self.g2t_module.tokenizer.pad_token_id
                
                gen_output = self.g2t_module.model.generate(
                    input_ids=inputs_encoding['input_ids'],
                    attention_mask=inputs_encoding['attention_mask'],
                    max_length=self.g2t_module.eval_max_length,
                    num_beams=self.g2t_module.eval_beams,
                    length_penalty=1.0,
                    early_stopping=True,
                )
        except Exception:
            print(inputs)
            raise

        return gen_output
    
    '''
    We create this function as an alteration from [this one](https://github.com/huggingface/transformers/blob/198c335d219a5eb4d3f124fdd1ce1a9cd9f78a9b/src/transformers/tokenization_utils_fast.py#L537), mainly because the official 'tokenizer.decode' treats all special tokens the same, while we want to drop all special tokens from the decoded sentence EXCEPT for the <unk> token, which we will replace later on.
    '''
    def __decode_ids_to_string_custom(
        self, token_ids: List[int], skip_special_tokens: bool = False, clean_up_tokenization_spaces: bool = True
    ) -> str:
        filtered_tokens = self.tokenizer.convert_ids_to_tokens(token_ids, skip_special_tokens=False)
        # Do not remove special tokens yet

        # To avoid mixing byte-level and unicode for byte-level BPT
        # we need to build string separatly for added tokens and byte-level tokens
        # cf. https://github.com/huggingface/transformers/issues/1133
        sub_texts = []
        current_sub_text = []
        for token in filtered_tokens:
            if skip_special_tokens and\
                token != self.tokenizer.unk_token and\
                token in self.tokenizer.all_special_tokens:

                continue
            else:
                current_sub_text.append(token)
        if current_sub_text:
            sub_texts.append(self.tokenizer.convert_tokens_to_string(current_sub_text))
        text = " ".join(sub_texts)

        if clean_up_tokenization_spaces:
            clean_text = self.tokenizer.clean_up_tokenization(text)
            return clean_text
        else:
            return text

    def __decode_sentences(self, encoded_sentences: Union[str, List[str]]):
        if type(encoded_sentences) == str:
            encoded_sentences = [encoded_sentences]
        decoded_sentences = [self.__decode_ids_to_string_custom(i, skip_special_tokens=True) for i in encoded_sentences]
        return decoded_sentences
        
    def verbalise_sentence(self, inputs: Union[str, List[str]]):
        if type(inputs) == str:
            inputs = [inputs]
        
        gen_output = self.__generate_verbalisations_from_inputs(inputs)
        
        decoded_sentences = self.__decode_sentences(gen_output)

        if len(decoded_sentences) == 1:
            return decoded_sentences[0]
        else:
            return decoded_sentences

    def verbalise_triples(self, input_triples: Union[Dict[str, str], List[Dict[str, str]], List[List[Dict[str, str]]]]):
        if type(input_triples) == dict:
            input_triples = [input_triples]

        verbalisation_inputs = []
        for triple in input_triples:
            if type(triple) == dict:
                assert 'subject' in triple
                assert 'predicate' in triple
                assert 'object' in triple
                verbalisation_inputs.append(
                    f'translate Graph to English: <H> {triple["subject"]} <R> {triple["predicate"]} <T> {triple["object"]}'
                )
            elif type(triple) == list:
                input_sentence = ['translate Graph to English:']
                for subtriple in triple:
                    assert 'subject' in subtriple
                    assert 'predicate' in subtriple
                    assert 'object' in subtriple
                    input_sentence.append(f'<H> {subtriple["subject"]}')
                    input_sentence.append(f'<R> {subtriple["predicate"]}')
                    input_sentence.append(f'<T> {subtriple["object"]}')
                verbalisation_inputs.append(
                    ' '.join(input_sentence)
                )

        return self.verbalise_sentence(verbalisation_inputs)
        
    def verbalise(self, input: Union[str, List, Dict]):
        try:
            if (type(input) == str) or (type(input) == list and type(input[0]) == str):
                return self.verbalise_sentence(input)
            elif (type(input) == dict) or (type(input) == list and type(input[0]) == dict):
                return self.verbalise_triples(input)
            else:
                return self.verbalise_triples(input)
        except Exception:
            print(f'ERROR VERBALISING {input}')
            raise
                
    def add_label_to_unk_replacer(self, label: str):
        N = self.unk_char_replace_sliding_window_size
        self.unknowns.append({})
        
        # Some pre-processing of labels to normalise some characters
        if self.convert_some_japanese_characters:
            label = label.replace('（','(')
            label = label.replace('）',')')
            label = label.replace('〈','<')
            label = label.replace('／','/')
            label = label.replace('〉','>')        
        
        label_encoded = self.tokenizer.encode(label)
        label_tokens = self.tokenizer.convert_ids_to_tokens(label_encoded)
        
        # Here, we also remove </s> (eos) and <pad> tokens in the replacing key, because:
        # 1) When the whole label is all unk:
        #   label_token_to_string would be '<unk></s>', meaning the replacing key (which is the same) only replaces
        #   the <unk> if it appears at the end of the sentence, which is not the desired effect.
        #   But since this means ANY <unk> will be replaced by this, it would be good to only replace keys that are <unk>
        #   on the last replacing pass.
        # 2) On other cases, then the unk is in the label but not in its entirety, like in the start/end, it might
        #   involve the starting <pad> token or the ending <eos> token on the replacing key, again forcing the replacement
        #   to only happen if the label appears in the end of the sentence.
        label_tokens = [t for t in label_tokens if t not in [
            self.tokenizer.eos_token, self.tokenizer.pad_token
        ]]

        label_token_to_string = self.tokenizer.convert_tokens_to_string(label_tokens)
        unk_token_to_string = self.tokenizer.convert_tokens_to_string([self.tokenizer.unk_token])
                
        #print(label_encoded,label_tokens,label_token_to_string)
        
        match_unks_in_label = re.findall('(?:(?: )*<unk>(?: )*)+', label_token_to_string)
        if len(match_unks_in_label) > 0:
            # If the whole label is made of UNK
            if (match_unks_in_label[0]) == label_token_to_string:
                #print('Label is all unks')                    
                self.unknowns[-1][label_token_to_string.strip()] = label
            # Else, there should be non-UNK characters in the label
            else:
                #print('Label is NOT all unks')
                # Analyse the label with a sliding window of size N (N before, N ahead)
                for idx, token in enumerate(label_tokens):
                    idx_before = max(0,idx-N)
                    idx_ahead = min(len(label_tokens), idx+N+1)
                    
                                       
                    # Found a UNK
                    if token == self.tokenizer.unk_token:
                        
                        # In case multiple UNK, exclude UNKs seen after this one, expand window to other side if possible
                        if len(match_unks_in_label) > 1:
                            #print(idx)
                            #print(label_tokens)
                            #print(label_tokens[idx_before:idx_ahead])
                            #print('HERE!')
                            # Reduce on the right, expanding on the left
                            while self.tokenizer.unk_token in label_tokens[idx+1:idx_ahead]:
                                idx_before = max(0,idx_before-1)
                                idx_ahead = min(idx+2, idx_ahead-1)
                                #print(label_tokens[idx_before:idx_ahead])
                            # Now just reduce on the left
                            while self.tokenizer.unk_token in label_tokens[idx_before:idx]:
                                idx_before = min(idx-1,idx_before+2)
                                #print(label_tokens[idx_before:idx_ahead])

                        span = self.tokenizer.convert_tokens_to_string(label_tokens[idx_before:idx_ahead])        
                        # First token of the label is UNK                        
                        if idx == 1 and label_tokens[0] == '▁':
                            #print('Label begins with unks')
                            to_replace = '^' + re.escape(span).replace(
                                    re.escape(unk_token_to_string),
                                    '.+?'
                                )
                            
                            replaced_span = re.search(
                                to_replace,
                                label
                            )[0]
                            self.unknowns[-1][span.strip()] = replaced_span
                        # Last token of the label is UNK
                        elif idx == len(label_tokens)-2 and label_tokens[-1] == self.tokenizer.eos_token:
                            #print('Label ends with unks')
                            pre_idx = self.tokenizer.convert_tokens_to_string(label_tokens[idx_before:idx])
                            pre_idx_unk_counts = pre_idx.count(unk_token_to_string)
                            to_replace = re.escape(span).replace(
                                    re.escape(unk_token_to_string),
                                    f'[^{re.escape(pre_idx)}]+?'
                                ) + '$'
                            
                            if pre_idx.strip() == '':
                                to_replace = to_replace.replace('[^]', '(?<=\s)[^a-zA-Z0-9]')
                            
                            replaced_span = re.search(
                                to_replace,
                                label
                            )[0]
                            self.unknowns[-1][span.strip()] = replaced_span
                            
                        # A token in-between the label is UNK                            
                        else:
                            #print('Label has unks in the middle')
                            pre_idx = self.tokenizer.convert_tokens_to_string(label_tokens[idx_before:idx])

                            to_replace = re.escape(span).replace(
                                re.escape(unk_token_to_string),
                                f'[^{re.escape(pre_idx)}]+?'
                            )
                            #If there is nothing behind the ??, because it is in the middle but the previous token is also
                            #a ??, then we would end up with to_replace beginning with [^], which we can't have
                            if pre_idx.strip() == '':
                                to_replace = to_replace.replace('[^]', '(?<=\s)[^a-zA-Z0-9]')
        
                            replaced_span = re.search(
                                to_replace,
                                label
                            )
                            
                            if replaced_span:
                                span = re.sub(r'\s([?.!",](?:\s|$))', r'\1', span.strip())
                                self.unknowns[-1][span] = replaced_span[0]  

    def replace_unks_on_sentence(self, sentence: str, loop_n : int = 3, empty_after : bool = False):
        # Loop through in case the labels are repeated, maximum of three times
        while '<unk>' in sentence and loop_n > 0:
            loop_n -= 1
            for unknowns in self.unknowns:
                for k,v in unknowns.items():
                    # Leave to replace all-unk labels at the last pass
                    if k == '<unk>' and loop_n > 0:
                        continue
                    # In case it is because the first letter of the sentence has been uppercased
                    if not k in sentence and k[0] == k[0].lower() and k[0].upper() == sentence[0]:
                        k = k[0].upper() + k[1:]
                        v = v[0].upper() + v[1:]
                    # In case it is because a double space is found where it should not be
                    elif not k in sentence and len(re.findall(r'\s{2,}',k))>0:
                        k = re.sub(r'\s+', ' ', k)
                    #print(k,'/',v,'/',sentence)
                    sentence = sentence.replace(k.strip(),v.strip(),1)
                    #sentence = re.sub(k, v, sentence)
            # Removing final doublespaces
            sentence = re.sub(r'\s+', ' ', sentence).strip()
            # Removing spaces before punctuation
            sentence = re.sub(r'\s([?.!",](?:\s|$))', r'\1', sentence)
        if empty_after:
            self.unknowns = []
        return sentence

if __name__ == '__main__':

    verb_module = VerbModule()
    verbs = verb_module.verbalise('translate Graph to English: <H> World Trade Center <R> height <T> 200 meter <H> World Trade Center <R> is a <T> tower')
    print(verbs)
